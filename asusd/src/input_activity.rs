//! Monitors keyboard, mouse, and trackpad input activity via evdev so other
//! tasks (e.g. keyboard backlight auto-off) can react to user idle time.
use std::collections::HashMap;
use std::os::fd::{AsRawFd, RawFd};
use std::path::PathBuf;

use evdev::{Device, EventType, KeyCode};
use log::{debug, warn};
use mio::unix::SourceFd;
use mio::{Events, Interest, Poll, Token};

/// Returns true if the device looks like a keyboard, mouse, or
/// trackpad/touchpad, i.e. something a user would use to directly interact
/// with the laptop.
fn is_relevant_input_device(device: &Device) -> bool {
    let supported = device.supported_events();
    let is_pointer =
        supported.contains(EventType::RELATIVE) || supported.contains(EventType::ABSOLUTE);
    // Heuristic to exclude single/few-button devices such as power or lid
    // switches: real keyboards support common alphanumeric keys.
    let is_keyboard = device
        .supported_keys()
        .is_some_and(|keys| keys.contains(KeyCode::KEY_ENTER));
    is_pointer || is_keyboard
}

fn open_relevant_devices() -> HashMap<PathBuf, Device> {
    let mut devices = HashMap::new();
    for (path, device) in evdev::enumerate() {
        if is_relevant_input_device(&device) {
            debug!(
                "Input activity monitor: watching {path:?} ({:?})",
                device.name().unwrap_or("unknown")
            );
            devices.insert(path, device);
        }
    }
    devices
}

static ACTIVITY_MONITOR: tokio::sync::OnceCell<Option<tokio::sync::watch::Receiver<()>>> =
    tokio::sync::OnceCell::const_new();

/// Start the input activity monitor on first use, shared by every caller.
///
/// The returned channel is notified (the value itself carries no meaning)
/// whenever any watched keyboard, mouse, or trackpad device reports an event.
pub async fn activity_receiver() -> Option<tokio::sync::watch::Receiver<()>> {
    ACTIVITY_MONITOR
        .get_or_init(start_activity_monitor)
        .await
        .clone()
}

/// Watch keyboard/mouse/trackpad evdev nodes for activity, and udev for
/// hotplugged input devices, reporting via a coalescing watch channel.
async fn start_activity_monitor() -> Option<tokio::sync::watch::Receiver<()>> {
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();

    std::thread::spawn(move || {
        let mut udev_monitor = match udev::MonitorBuilder::new()
            .and_then(|m| m.match_subsystem("input"))
            .and_then(|m| m.listen())
        {
            Ok(m) => m,
            Err(e) => {
                warn!("Could not create a udev input monitor: {e}");
                let _ = ready_tx.send(None);
                return;
            }
        };

        let mut poll = match Poll::new() {
            Ok(p) => p,
            Err(e) => {
                warn!("Could not create a mio poll for the input activity monitor: {e}");
                let _ = ready_tx.send(None);
                return;
            }
        };

        const UDEV_TOKEN: Token = Token(0);
        if let Err(e) =
            poll.registry()
                .register(&mut udev_monitor, UDEV_TOKEN, Interest::READABLE)
        {
            warn!("Could not register the udev input monitor with mio: {e}");
            let _ = ready_tx.send(None);
            return;
        }

        let mut devices = open_relevant_devices();
        let mut next_token = 1usize;
        let mut token_paths: HashMap<Token, PathBuf> = HashMap::new();

        for (path, device) in devices.iter() {
            let token = Token(next_token);
            next_token += 1;
            let fd: RawFd = device.as_raw_fd();
            if poll
                .registry()
                .register(&mut SourceFd(&fd), token, Interest::READABLE)
                .is_ok()
            {
                token_paths.insert(token, path.clone());
            }
        }

        let (tx, rx) = tokio::sync::watch::channel(());
        let _ = ready_tx.send(Some(rx));

        let mut events = Events::with_capacity(32);
        loop {
            match poll.poll(&mut events, None) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    warn!(
                        "Input activity monitor poll error, input activity will no longer be \
                         detected: {e}"
                    );
                    return;
                }
            }

            let mut activity = false;
            let mut rescan = false;
            for event in &events {
                if event.token() == UDEV_TOKEN {
                    for udev_event in udev_monitor.iter() {
                        if matches!(
                            udev_event.event_type(),
                            udev::EventType::Add | udev::EventType::Remove
                        ) {
                            rescan = true;
                        }
                    }
                } else if let Some(path) = token_paths.get(&event.token()) {
                    if let Some(device) = devices.get_mut(path) {
                        match device.fetch_events() {
                            Ok(iter) => {
                                if iter.count() > 0 {
                                    activity = true;
                                }
                            }
                            Err(e) => {
                                debug!("Input activity monitor: could not read {path:?}: {e}");
                            }
                        }
                    }
                }
            }

            if rescan {
                for device in devices.values() {
                    let fd: RawFd = device.as_raw_fd();
                    poll.registry().deregister(&mut SourceFd(&fd)).ok();
                }
                devices = open_relevant_devices();
                token_paths.clear();
                for (path, device) in devices.iter() {
                    let token = Token(next_token);
                    next_token += 1;
                    let fd: RawFd = device.as_raw_fd();
                    if poll
                        .registry()
                        .register(&mut SourceFd(&fd), token, Interest::READABLE)
                        .is_ok()
                    {
                        token_paths.insert(token, path.clone());
                    }
                }
            }

            if activity {
                tx.send_replace(());
            }
        }
    });

    ready_rx.await.ok().flatten()
}
