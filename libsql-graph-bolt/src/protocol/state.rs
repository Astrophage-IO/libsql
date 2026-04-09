#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoltState {
    Negotiation,
    Ready,
    Streaming,
    TxReady,
    TxStreaming,
    Failed,
    Interrupted,
    Defunct,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestKind {
    Hello,
    Goodbye,
    Reset,
    Run,
    Begin,
    Commit,
    Rollback,
    Discard,
    Pull,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransitionResult {
    NewState(BoltState),
    Ignored,
    Invalid,
}

impl BoltState {
    pub fn transition(
        &self,
        request: RequestKind,
        success: bool,
        has_more: bool,
    ) -> TransitionResult {
        use BoltState::*;
        use RequestKind::*;
        use TransitionResult::*;

        match self {
            Negotiation => match request {
                Hello if success => NewState(Ready),
                Hello => NewState(Defunct),
                _ => Invalid,
            },

            Ready => match request {
                Run if success => NewState(Streaming),
                Run => NewState(Failed),
                Begin if success => NewState(TxReady),
                Begin => NewState(Failed),
                Reset if success => NewState(Ready),
                Reset => NewState(Defunct),
                Goodbye => NewState(Defunct),
                _ => Invalid,
            },

            Streaming => match request {
                Pull if success && has_more => NewState(Streaming),
                Pull if success => NewState(Ready),
                Pull => NewState(Failed),
                Discard if success && has_more => NewState(Streaming),
                Discard if success => NewState(Ready),
                Discard => NewState(Failed),
                Reset if success => NewState(Ready),
                Reset => NewState(Defunct),
                Goodbye => NewState(Defunct),
                _ => Invalid,
            },

            TxReady => match request {
                Run if success => NewState(TxStreaming),
                Run => NewState(Failed),
                Commit if success => NewState(Ready),
                Commit => NewState(Failed),
                Rollback if success => NewState(Ready),
                Rollback => NewState(Failed),
                Reset if success => NewState(Ready),
                Reset => NewState(Defunct),
                Goodbye => NewState(Defunct),
                _ => Invalid,
            },

            TxStreaming => match request {
                Pull if success && has_more => NewState(TxStreaming),
                Pull if success => NewState(TxReady),
                Pull => NewState(Failed),
                Discard if success && has_more => NewState(TxStreaming),
                Discard if success => NewState(TxReady),
                Discard => NewState(Failed),
                Run if success => NewState(TxStreaming),
                Run => NewState(Failed),
                Reset if success => NewState(Ready),
                Reset => NewState(Defunct),
                Goodbye => NewState(Defunct),
                _ => Invalid,
            },

            Failed => match request {
                Reset if success => NewState(Ready),
                Reset => NewState(Defunct),
                Goodbye => NewState(Defunct),
                _ => Ignored,
            },

            Interrupted => match request {
                Reset if success => NewState(Ready),
                Reset => NewState(Defunct),
                Goodbye => NewState(Defunct),
                _ => Ignored,
            },

            Defunct => Invalid,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use BoltState::*;
    use RequestKind::*;
    use TransitionResult::*;

    fn t(state: BoltState, req: RequestKind, success: bool, has_more: bool) -> TransitionResult {
        state.transition(req, success, has_more)
    }

    #[test]
    fn negotiation_hello_ok() {
        assert_eq!(t(Negotiation, Hello, true, false), NewState(Ready));
    }

    #[test]
    fn negotiation_hello_fail() {
        assert_eq!(t(Negotiation, Hello, false, false), NewState(Defunct));
    }

    #[test]
    fn negotiation_rejects_other_requests() {
        for req in [Goodbye, Reset, Run, Begin, Commit, Rollback, Discard, Pull] {
            assert_eq!(t(Negotiation, req, true, false), Invalid);
        }
    }

    #[test]
    fn ready_run_ok() {
        assert_eq!(t(Ready, Run, true, false), NewState(Streaming));
    }

    #[test]
    fn ready_run_fail() {
        assert_eq!(t(Ready, Run, false, false), NewState(Failed));
    }

    #[test]
    fn ready_begin_ok() {
        assert_eq!(t(Ready, Begin, true, false), NewState(TxReady));
    }

    #[test]
    fn ready_begin_fail() {
        assert_eq!(t(Ready, Begin, false, false), NewState(Failed));
    }

    #[test]
    fn ready_reset_ok() {
        assert_eq!(t(Ready, Reset, true, false), NewState(Ready));
    }

    #[test]
    fn ready_reset_fail() {
        assert_eq!(t(Ready, Reset, false, false), NewState(Defunct));
    }

    #[test]
    fn ready_goodbye() {
        assert_eq!(t(Ready, Goodbye, true, false), NewState(Defunct));
    }

    #[test]
    fn ready_rejects_invalid() {
        for req in [Hello, Commit, Rollback, Discard, Pull] {
            assert_eq!(t(Ready, req, true, false), Invalid);
        }
    }

    #[test]
    fn streaming_pull_ok_has_more() {
        assert_eq!(t(Streaming, Pull, true, true), NewState(Streaming));
    }

    #[test]
    fn streaming_pull_ok_done() {
        assert_eq!(t(Streaming, Pull, true, false), NewState(Ready));
    }

    #[test]
    fn streaming_pull_fail() {
        assert_eq!(t(Streaming, Pull, false, false), NewState(Failed));
    }

    #[test]
    fn streaming_discard_ok_has_more() {
        assert_eq!(t(Streaming, Discard, true, true), NewState(Streaming));
    }

    #[test]
    fn streaming_discard_ok_done() {
        assert_eq!(t(Streaming, Discard, true, false), NewState(Ready));
    }

    #[test]
    fn streaming_discard_fail() {
        assert_eq!(t(Streaming, Discard, false, false), NewState(Failed));
    }

    #[test]
    fn streaming_reset_ok() {
        assert_eq!(t(Streaming, Reset, true, false), NewState(Ready));
    }

    #[test]
    fn streaming_reset_fail() {
        assert_eq!(t(Streaming, Reset, false, false), NewState(Defunct));
    }

    #[test]
    fn streaming_goodbye() {
        assert_eq!(t(Streaming, Goodbye, true, false), NewState(Defunct));
    }

    #[test]
    fn streaming_rejects_invalid() {
        for req in [Hello, Run, Begin, Commit, Rollback] {
            assert_eq!(t(Streaming, req, true, false), Invalid);
        }
    }

    #[test]
    fn tx_ready_run_ok() {
        assert_eq!(t(TxReady, Run, true, false), NewState(TxStreaming));
    }

    #[test]
    fn tx_ready_run_fail() {
        assert_eq!(t(TxReady, Run, false, false), NewState(Failed));
    }

    #[test]
    fn tx_ready_commit_ok() {
        assert_eq!(t(TxReady, Commit, true, false), NewState(Ready));
    }

    #[test]
    fn tx_ready_commit_fail() {
        assert_eq!(t(TxReady, Commit, false, false), NewState(Failed));
    }

    #[test]
    fn tx_ready_rollback_ok() {
        assert_eq!(t(TxReady, Rollback, true, false), NewState(Ready));
    }

    #[test]
    fn tx_ready_rollback_fail() {
        assert_eq!(t(TxReady, Rollback, false, false), NewState(Failed));
    }

    #[test]
    fn tx_ready_reset_ok() {
        assert_eq!(t(TxReady, Reset, true, false), NewState(Ready));
    }

    #[test]
    fn tx_ready_reset_fail() {
        assert_eq!(t(TxReady, Reset, false, false), NewState(Defunct));
    }

    #[test]
    fn tx_ready_goodbye() {
        assert_eq!(t(TxReady, Goodbye, true, false), NewState(Defunct));
    }

    #[test]
    fn tx_ready_rejects_invalid() {
        for req in [Hello, Begin, Discard, Pull] {
            assert_eq!(t(TxReady, req, true, false), Invalid);
        }
    }

    #[test]
    fn tx_streaming_pull_ok_has_more() {
        assert_eq!(t(TxStreaming, Pull, true, true), NewState(TxStreaming));
    }

    #[test]
    fn tx_streaming_pull_ok_done() {
        assert_eq!(t(TxStreaming, Pull, true, false), NewState(TxReady));
    }

    #[test]
    fn tx_streaming_pull_fail() {
        assert_eq!(t(TxStreaming, Pull, false, false), NewState(Failed));
    }

    #[test]
    fn tx_streaming_discard_ok_has_more() {
        assert_eq!(t(TxStreaming, Discard, true, true), NewState(TxStreaming));
    }

    #[test]
    fn tx_streaming_discard_ok_done() {
        assert_eq!(t(TxStreaming, Discard, true, false), NewState(TxReady));
    }

    #[test]
    fn tx_streaming_discard_fail() {
        assert_eq!(t(TxStreaming, Discard, false, false), NewState(Failed));
    }

    #[test]
    fn tx_streaming_run_ok() {
        assert_eq!(t(TxStreaming, Run, true, false), NewState(TxStreaming));
    }

    #[test]
    fn tx_streaming_run_fail() {
        assert_eq!(t(TxStreaming, Run, false, false), NewState(Failed));
    }

    #[test]
    fn tx_streaming_reset_ok() {
        assert_eq!(t(TxStreaming, Reset, true, false), NewState(Ready));
    }

    #[test]
    fn tx_streaming_reset_fail() {
        assert_eq!(t(TxStreaming, Reset, false, false), NewState(Defunct));
    }

    #[test]
    fn tx_streaming_goodbye() {
        assert_eq!(t(TxStreaming, Goodbye, true, false), NewState(Defunct));
    }

    #[test]
    fn tx_streaming_rejects_invalid() {
        for req in [Hello, Begin, Commit, Rollback] {
            assert_eq!(t(TxStreaming, req, true, false), Invalid);
        }
    }

    #[test]
    fn failed_reset_ok() {
        assert_eq!(t(Failed, Reset, true, false), NewState(Ready));
    }

    #[test]
    fn failed_reset_fail() {
        assert_eq!(t(Failed, Reset, false, false), NewState(Defunct));
    }

    #[test]
    fn failed_goodbye() {
        assert_eq!(t(Failed, Goodbye, true, false), NewState(Defunct));
    }

    #[test]
    fn failed_ignores_other_requests() {
        for req in [Hello, Run, Begin, Commit, Rollback, Discard, Pull] {
            assert_eq!(t(Failed, req, true, false), Ignored);
        }
    }

    #[test]
    fn interrupted_reset_ok() {
        assert_eq!(t(Interrupted, Reset, true, false), NewState(Ready));
    }

    #[test]
    fn interrupted_reset_fail() {
        assert_eq!(t(Interrupted, Reset, false, false), NewState(Defunct));
    }

    #[test]
    fn interrupted_goodbye() {
        assert_eq!(t(Interrupted, Goodbye, true, false), NewState(Defunct));
    }

    #[test]
    fn interrupted_ignores_other_requests() {
        for req in [Hello, Run, Begin, Commit, Rollback, Discard, Pull] {
            assert_eq!(t(Interrupted, req, true, false), Ignored);
        }
    }

    #[test]
    fn defunct_is_terminal() {
        for req in [
            Hello, Goodbye, Reset, Run, Begin, Commit, Rollback, Discard, Pull,
        ] {
            assert_eq!(t(Defunct, req, true, false), Invalid);
            assert_eq!(t(Defunct, req, false, false), Invalid);
        }
    }

    #[test]
    fn has_more_keeps_streaming() {
        assert_eq!(t(Streaming, Pull, true, true), NewState(Streaming));
        assert_eq!(t(Streaming, Discard, true, true), NewState(Streaming));
        assert_eq!(t(TxStreaming, Pull, true, true), NewState(TxStreaming));
        assert_eq!(t(TxStreaming, Discard, true, true), NewState(TxStreaming));
    }

    #[test]
    fn done_returns_to_parent_state() {
        assert_eq!(t(Streaming, Pull, true, false), NewState(Ready));
        assert_eq!(t(Streaming, Discard, true, false), NewState(Ready));
        assert_eq!(t(TxStreaming, Pull, true, false), NewState(TxReady));
        assert_eq!(t(TxStreaming, Discard, true, false), NewState(TxReady));
    }

    #[test]
    fn has_more_ignored_on_failure() {
        assert_eq!(t(Streaming, Pull, false, true), NewState(Failed));
        assert_eq!(t(Streaming, Discard, false, true), NewState(Failed));
        assert_eq!(t(TxStreaming, Pull, false, true), NewState(Failed));
        assert_eq!(t(TxStreaming, Discard, false, true), NewState(Failed));
    }

    #[test]
    fn full_autocommit_lifecycle() {
        let mut state = Negotiation;
        state = match t(state, Hello, true, false) {
            NewState(s) => s,
            _ => panic!(),
        };
        assert_eq!(state, Ready);
        state = match t(state, Run, true, false) {
            NewState(s) => s,
            _ => panic!(),
        };
        assert_eq!(state, Streaming);
        state = match t(state, Pull, true, false) {
            NewState(s) => s,
            _ => panic!(),
        };
        assert_eq!(state, Ready);
        state = match t(state, Goodbye, true, false) {
            NewState(s) => s,
            _ => panic!(),
        };
        assert_eq!(state, Defunct);
    }

    #[test]
    fn full_explicit_transaction_lifecycle() {
        let mut state = Negotiation;
        state = match t(state, Hello, true, false) {
            NewState(s) => s,
            _ => panic!(),
        };
        assert_eq!(state, Ready);
        state = match t(state, Begin, true, false) {
            NewState(s) => s,
            _ => panic!(),
        };
        assert_eq!(state, TxReady);
        state = match t(state, Run, true, false) {
            NewState(s) => s,
            _ => panic!(),
        };
        assert_eq!(state, TxStreaming);
        state = match t(state, Pull, true, false) {
            NewState(s) => s,
            _ => panic!(),
        };
        assert_eq!(state, TxReady);
        state = match t(state, Commit, true, false) {
            NewState(s) => s,
            _ => panic!(),
        };
        assert_eq!(state, Ready);
        state = match t(state, Goodbye, true, false) {
            NewState(s) => s,
            _ => panic!(),
        };
        assert_eq!(state, Defunct);
    }

    #[test]
    fn failure_recovery_lifecycle() {
        let mut state = Ready;
        state = match t(state, Run, false, false) {
            NewState(s) => s,
            _ => panic!(),
        };
        assert_eq!(state, Failed);
        assert_eq!(t(state, Run, true, false), Ignored);
        assert_eq!(t(state, Pull, true, false), Ignored);
        state = match t(state, Reset, true, false) {
            NewState(s) => s,
            _ => panic!(),
        };
        assert_eq!(state, Ready);
    }

    #[test]
    fn streaming_with_multiple_pulls() {
        let mut state = Streaming;
        state = match t(state, Pull, true, true) {
            NewState(s) => s,
            _ => panic!(),
        };
        assert_eq!(state, Streaming);
        state = match t(state, Pull, true, true) {
            NewState(s) => s,
            _ => panic!(),
        };
        assert_eq!(state, Streaming);
        state = match t(state, Pull, true, false) {
            NewState(s) => s,
            _ => panic!(),
        };
        assert_eq!(state, Ready);
    }

    #[test]
    fn tx_streaming_multiple_runs() {
        let mut state = TxStreaming;
        state = match t(state, Pull, true, false) {
            NewState(s) => s,
            _ => panic!(),
        };
        assert_eq!(state, TxReady);
        state = match t(state, Run, true, false) {
            NewState(s) => s,
            _ => panic!(),
        };
        assert_eq!(state, TxStreaming);
        state = match t(state, Run, true, false) {
            NewState(s) => s,
            _ => panic!(),
        };
        assert_eq!(state, TxStreaming);
    }
}
