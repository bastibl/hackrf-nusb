//! Small executable contract for explicit half-duplex streams.

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum Radio {
    #[default]
    Off,
    Rx,
    Tx,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum Stream {
    #[default]
    Stopped,
    Paused,
    Running,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Action {
    StartRx,
    StartTx,
    Read,
    Write,
    StopRx,
    StopTx,
    FailRead,
    FailWrite,
    Configure,
    DropRx,
    DropTx,
}

#[derive(Clone, Debug, Default)]
struct Model {
    radio: Radio,
    rx: Stream,
    tx: Stream,
    webusb: bool,
    rx_queue_abandoned: bool,
}

impl Model {
    fn apply(&mut self, action: Action) {
        match action {
            Action::StartRx
                if self.radio == Radio::Off
                    && !self.rx_queue_abandoned
                    && matches!(self.rx, Stream::Stopped | Stream::Paused) =>
            {
                self.radio = Radio::Rx;
                self.rx = Stream::Running;
            }
            Action::StartTx if self.radio == Radio::Off && self.tx == Stream::Stopped => {
                self.radio = Radio::Tx;
                self.tx = Stream::Running;
            }
            Action::Read if self.radio == Radio::Rx && self.rx == Stream::Running => {}
            Action::Write if self.radio == Radio::Tx && self.tx == Stream::Running => {}
            Action::StopRx if self.radio == Radio::Rx && self.rx == Stream::Running => {
                self.radio = Radio::Off;
                self.rx = Stream::Paused;
            }
            Action::StopTx if self.radio == Radio::Tx && self.tx == Stream::Running => {
                self.radio = Radio::Off;
                self.tx = Stream::Stopped;
            }
            Action::FailRead if self.radio == Radio::Rx && self.rx == Stream::Running => {
                self.radio = Radio::Off;
                self.rx = Stream::Failed;
            }
            Action::FailWrite if self.radio == Radio::Tx && self.tx == Stream::Running => {
                self.radio = Radio::Off;
                self.tx = Stream::Failed;
            }
            Action::Configure if matches!(self.radio, Radio::Off | Radio::Rx | Radio::Tx) => {}
            Action::DropRx if self.rx == Stream::Running => {
                self.radio = Radio::Off;
                self.rx = Stream::Stopped;
                self.rx_queue_abandoned = self.webusb;
            }
            Action::DropRx if matches!(self.rx, Stream::Paused | Stream::Failed) => {
                self.rx = Stream::Stopped;
                self.rx_queue_abandoned = self.webusb;
            }
            Action::DropTx if self.tx == Stream::Failed => self.tx = Stream::Stopped,
            _ => {}
        }
        self.assert_invariants();
    }

    fn assert_invariants(&self) {
        assert!(
            self.radio != Radio::Rx || self.rx == Stream::Running,
            "RX hardware requires a running RX stream: {self:#?}"
        );
        assert!(
            self.radio != Radio::Tx || self.tx == Stream::Running,
            "TX hardware requires a running TX stream: {self:#?}"
        );
        assert!(
            !(self.rx == Stream::Running && self.tx == Stream::Running),
            "half-duplex streams cannot both run: {self:#?}"
        );
    }
}

#[test]
fn all_short_explicit_traces_preserve_invariants() {
    const ACTIONS: &[Action] = &[
        Action::StartRx,
        Action::StartTx,
        Action::Read,
        Action::Write,
        Action::StopRx,
        Action::StopTx,
        Action::FailRead,
        Action::FailWrite,
        Action::Configure,
        Action::DropRx,
        Action::DropTx,
    ];

    fn explore(model: Model, depth: u8) {
        if depth == 0 {
            return;
        }
        for action in ACTIONS {
            let mut next = model.clone();
            next.apply(*action);
            explore(next, depth - 1);
        }
    }

    explore(Model::default(), 6);
}

#[test]
fn direction_change_requires_an_explicit_stop() {
    let mut model = Model::default();
    model.apply(Action::StartRx);
    model.apply(Action::StartTx);
    assert_eq!(model.radio, Radio::Rx);
    assert_eq!(model.tx, Stream::Stopped);

    model.apply(Action::StopRx);
    assert_eq!(model.rx, Stream::Paused);
    model.apply(Action::StartTx);
    assert_eq!(model.radio, Radio::Tx);
    assert_eq!(model.tx, Stream::Running);
}

#[test]
fn stopped_rx_resumes_after_tx_without_recreating_the_stream() {
    let mut model = Model::default();
    model.apply(Action::StartRx);
    model.apply(Action::StopRx);
    model.apply(Action::StartTx);
    model.apply(Action::StopTx);
    model.apply(Action::StartRx);

    assert_eq!(model.radio, Radio::Rx);
    assert_eq!(model.rx, Stream::Running);
}

#[test]
fn a_failed_stream_requires_replacement_before_restart() {
    let mut model = Model::default();
    model.apply(Action::StartTx);
    model.apply(Action::FailWrite);
    model.apply(Action::StartTx);
    assert_eq!(model.radio, Radio::Off);
    assert_eq!(model.tx, Stream::Failed);

    model.apply(Action::DropTx);
    model.apply(Action::StartTx);
    assert_eq!(model.radio, Radio::Tx);
}

#[test]
fn configuration_keeps_an_active_stream_running() {
    let mut model = Model::default();
    model.apply(Action::StartRx);
    model.apply(Action::Configure);
    assert_eq!(model.radio, Radio::Rx);
    assert_eq!(model.rx, Stream::Running);
}

#[test]
fn webusb_dropped_rx_queue_requires_reopening_the_device() {
    // Browsers do not expose cancellation for the submitted bulk-IN
    // transfers. Replacing this stream would create a second RX queue, so the
    // WebUSB implementation intentionally requires a fresh device session.
    let mut model = Model {
        webusb: true,
        ..Model::default()
    };
    model.apply(Action::StartRx);
    model.apply(Action::StopRx);
    model.apply(Action::DropRx);

    assert_eq!(model.radio, Radio::Off);
    assert_eq!(model.rx, Stream::Stopped);
    model.apply(Action::StartRx);
    assert_eq!(model.radio, Radio::Off);
    // A fresh `Model` stands in for dropping and reopening `Device`.
    let mut reopened = Model::default();
    reopened.apply(Action::StartRx);
    assert_eq!(reopened.radio, Radio::Rx);
}
