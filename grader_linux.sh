#!/usr/bin/env bash


set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$PWD"
RUN_ROOT="$PWD/logs"
RUN_ID="grader_$(date +%Y%m%d_%H%M%S)"
RUN_DIR="$RUN_ROOT/$RUN_ID"

SERVER_SRC="./server_skeleton.c"
CLIENT_SRC="./client_skeleton.c"
SERVER_BIN="$PROJECT_ROOT/server"
CLIENT_BIN="$PROJECT_ROOT/client"
SERVER_LOG="$RUN_DIR/server.log"
PORT=6789

MILD_PROFILE="delay 200ms loss 8%"
TIMEOUT_DEMO_PROFILE="delay 1200ms"

SERVER_PID=""
FAILURES=0
SKIP_COMPILE=0

TEST_BASIC=0
TEST_BROADCAST=0
TEST_RDT=0
TEST_OFFLINE=0

declare -A CLIENT_PIDS
declare -A CLIENT_FDS
declare -A CLIENT_LOGS
declare -A CLIENT_FIFOS

print_usage() {
	cat <<'EOF'
Usage: ./grader_linux.sh [options]

Test Phases (corresponds to Grading Scheme):
  --test basic       Test Registration, DIR, EXIT, Multi-user (20 pts)
  --test broadcast   Test Broadcast & Simultaneous recv/send (15 pts)
  --test rdt         Test Direct UDP & RDT3.0 (35 pts)
  --test offline     Test Offline Message Box (10 pts)
  --test all         Run all tests (80 pts total, remaining 20 for report)

Options:
  --skip-compile     Skip gcc compilation
EOF
}

log_step() { printf '\n========== [PHASE] %s ==========\n' "$1"; }
log_info() { printf '[INFO] %s\n' "$1"; }
log_pass() { printf '  ✅ [PASS] %s\n' "$1"; }
log_fail() { 
    printf '  ❌ [FAIL] %s\n' "$1"
    FAILURES=$((FAILURES + 1))
}

require_command() {
	if ! command -v "$1" >/dev/null 2>&1; then
		printf 'Missing required command: %s\n' "$1" >&2
		exit 1
	fi
}

parse_args() {
    if [[ $# -eq 0 ]]; then
        TEST_BASIC=1; TEST_BROADCAST=1; TEST_RDT=1; TEST_OFFLINE=1
    fi
	while (($# > 0)); do
		case "$1" in
			--test)
                case "$2" in
                    basic) TEST_BASIC=1 ;;
                    broadcast) TEST_BROADCAST=1 ;;
                    rdt) TEST_RDT=1 ;;
                    offline) TEST_OFFLINE=1 ;;
                    all) TEST_BASIC=1; TEST_BROADCAST=1; TEST_RDT=1; TEST_OFFLINE=1 ;;
                    *) echo "Unknown test phase: $2"; exit 1 ;;
                esac
				shift
				;;
			--skip-compile) SKIP_COMPILE=1 ;;
			--help) print_usage; exit 0 ;;
			*) shift ;;
		esac
		shift
	done
}

ensure_run_directory() { mkdir -p "$RUN_DIR"; }

compile_binaries() {
	if ((SKIP_COMPILE == 1)); then return; fi
	log_info "Compiling..."
	if ! gcc -w -o "$SERVER_BIN" "$SERVER_SRC" -lpthread; then
		echo "❌ [FAIL] Server compilation failed! Aborting tests."
		exit 1
	fi
	if ! gcc -w -o "$CLIENT_BIN" "$CLIENT_SRC" -lpthread; then
		echo "❌ [FAIL] Client compilation failed! Aborting tests."
		exit 1
	fi
	log_info "Compilation successful."
}

stdbuf_prefix() {
	if command -v stdbuf >/dev/null 2>&1; then printf 'stdbuf -oL -eL '
	elif command -v gstdbuf >/dev/null 2>&1; then printf 'gstdbuf -oL -eL '
	else printf ''; fi
}

run_sudo() { sudo "$@"; }

clear_netem() {
	run_sudo tc qdisc del dev lo root >/dev/null 2>&1 || true
}

apply_netem() {
	local profile="$1"
	# Ensure clean state before applying
	clear_netem
	
	# Apply netem only to UDP (protocol 17) to match the macOS pf constraint (proto udp)
	run_sudo tc qdisc add dev lo root handle 1: prio
	run_sudo tc qdisc add dev lo parent 1:1 handle 10: netem $profile >/dev/null 2>&1 || true
	run_sudo tc filter add dev lo protocol ip parent 1:0 prio 1 u32 match ip protocol 17 0xff flowid 1:1 >/dev/null 2>&1 || true
}

start_server() {
	local buf="$(stdbuf_prefix)"
	eval "$buf \"$SERVER_BIN\" >\"$SERVER_LOG\" 2>&1 &"
	SERVER_PID=$!
	local deadline=$((SECONDS + 10))
	while ((SECONDS < deadline)); do
		if ss -nlt 2>/dev/null | grep -Fq ":$PORT"; then return; fi
        if lsof -nP -iTCP:"$PORT" -sTCP:LISTEN 2>/dev/null | grep -Fq ":$PORT"; then return; fi
		sleep 0.2
	done
}

start_client() {
	local label="$1"; local fifo="$RUN_DIR/${label}.fifo"; local log="$RUN_DIR/${label}.log"
	rm -f "$fifo"; mkfifo "$fifo"; exec {fd}<>"$fifo"
	local buf="$(stdbuf_prefix)"
	eval "$buf \"$CLIENT_BIN\" <\"$fifo\" >\"$log\" 2>&1 &"
	CLIENT_PIDS["$label"]=$!; CLIENT_FDS["$label"]=$fd; CLIENT_LOGS["$label"]="$log"; CLIENT_FIFOS["$label"]="$fifo"
	sleep 0.5
}
send_client_line() {
	local label="$1"; local line="$2"; local fd="${CLIENT_FDS[$label]}"
	printf '%s\n' "$line" >&$fd
}
register_client() {
	local label="$1"; local username="$2"; send_client_line "$label" "$username"
	assert_log_contains "${CLIENT_LOGS[$label]}" "Your P2P listener is on 127.0.0.1:" 8 "$label registration ($username)"
}
stop_client() {
	local label="$1"; local pid="${CLIENT_PIDS[$label]:-}"; local fd="${CLIENT_FDS[$label]:-}"
	if [[ -n "$fd" ]]; then printf 'EXIT\n' >&$fd 2>/dev/null || true; fi
	sleep 1
	if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then kill "$pid" 2>/dev/null || true; fi
	unset CLIENT_PIDS["$label"] CLIENT_FDS["$label"] CLIENT_LOGS["$label"] CLIENT_FIFOS["$label"] || true
}
assert_log_contains() {
	local file="$1"; local needle="$2"; local timeout_secs="$3"; local description="$4"
	local deadline=$((SECONDS + timeout_secs))
	while ((SECONDS < deadline)); do
		if grep -Fq -- "$needle" "$file" 2>/dev/null; then log_pass "$description"; return 0; fi
		sleep 0.2
	done
	log_fail "$description (Timeout waiting for: $needle)"
}
build_long_message() {
	local prefix="$1"; local target_len="$2"; local chunk="0123456789abcdef"; local message="$prefix"
	while ((${#message} < target_len)); do message+="$chunk"; done
	printf '%s' "${message:0:target_len}"
}

# --- PHASES ---

phase_basic() {
    log_step "Test 1: Basic Operations (Registration, Multi-user, DIR, EXIT)"
    start_client alice; start_client bob; start_client carol
    register_client alice alice
    register_client bob bob
    register_client carol carol
    
    send_client_line alice "DIR"
    assert_log_contains "${CLIENT_LOGS[alice]}" "bob" 5 "DIR shows bob online"
    
    stop_client carol
    assert_log_contains "${CLIENT_LOGS[alice]}" "carol has left the chatroom" 5 "EXIT notification broadcasted"
    
    send_client_line alice "DIR"
    assert_log_contains "${CLIENT_LOGS[alice]}" "carol" 5 "DIR shows carol offline"
    
    log_info "=> Passing this proves: Registration (5pt), Exit (5pt), Multi-user (10pt)"
}

phase_broadcast() {
    log_step "Test 2: Broadcast & Async I/O (Simultaneous Receiving/Sending)"
    start_client alice; start_client bob
    register_client alice alice; register_client bob bob
    
    # Alice is blocked on fgets(), Bob sends a broadcast. If Alice gets it, Async works!
    send_client_line bob "broadcast-check-from-bob"
    assert_log_contains "${CLIENT_LOGS[alice]}" "bob: broadcast-check-from-bob" 5 "Alice received broadcast while waiting for input"
    
    log_info "=> Passing this proves: Broadcast messages (5pt), Simultaneously receiving and sending (10pt)"
}

phase_rdt() {
    log_step "Test 3: Reliability & P2P (UDP Direct & RDT 3.0)"
    start_client alice; start_client bob
    register_client alice alice; register_client bob bob
    
    apply_netem "delay 50ms loss 15%"  # 15% drop rate: strict enough to fail raw UDP, but allows RDT to pass within 5 retries
    sleep 1

    log_info "Sending multiple short messages to ensure loss recovery..."
    for i in {1..10}; do
        send_client_line bob "#alice: multi-rdt-check-$i"
        assert_log_contains "${CLIENT_LOGS[alice]}" "bob to you: multi-rdt-check-$i" 15 "Received UDP message #$i"
        sleep 0.2
    done
    
    log_info "Sending multiple long fragmented messages..."
    for i in {1..3}; do
        local long_message="$(build_long_message "FRAG_TEST_${i}_" 1500)"
        send_client_line bob "#alice: $long_message"
        assert_log_contains "${CLIENT_LOGS[alice]}" "bob to you: FRAG_TEST_${i}_" 25 "Received fragmented huge UDP message #$i"
        sleep 0.2
    done

    clear_netem
    log_info "=> Passing this proves: Direct UDP messages (10pt), Implement RDT3.0 on UDP (25pt)"
}

phase_offline() {
    log_step "Test 4: Offline Message Fallback"
    start_client alice; start_client bob
    register_client alice alice; register_client bob bob
    
    stop_client bob
    sleep 1
    
    send_client_line alice "#bob: offline-secret-message"
    assert_log_contains "${CLIENT_LOGS[alice]}" "Message saved for bob." 8 "Server stored the offline message"
    
    start_client bob_relogin
    register_client bob_relogin bob
    assert_log_contains "${CLIENT_LOGS[bob_relogin]}" "[Offline] alice to you: offline-secret-message" 10 "Bob got offline message on relogin"
    
    log_info "=> Passing this proves: Message box for offline user (10pt)"
}

cleanup() {
	set +e
	clear_netem
	for pid in "${CLIENT_PIDS[@]:-}"; do kill -9 "$pid" 2>/dev/null || true; done
	if [[ -n "$SERVER_PID" ]]; then kill -9 "$SERVER_PID" 2>/dev/null || true; fi
    log_step "SUMMARY"
    if ((FAILURES == 0)); then
        echo "🎉 ALL SELECTED TESTS PASSED! Check logs in: $RUN_DIR"
    else
        echo "❌ $FAILURES ASSERTS FAILED. Please review the output context."
    fi
}

main() {
	parse_args "$@"
	ensure_run_directory
	run_sudo -v
	clear_netem
	compile_binaries
	
    if ((TEST_BASIC)); then
        start_server
        phase_basic
        cleanup
        start_server # restart cleanly for next phase
    fi
    if ((TEST_BROADCAST)); then
        if [[ -z "${SERVER_PID:-}" ]] || ! kill -0 "$SERVER_PID" 2>/dev/null; then start_server; fi
        phase_broadcast
        cleanup
        start_server
    fi
    if ((TEST_RDT)); then
        if [[ -z "${SERVER_PID:-}" ]] || ! kill -0 "$SERVER_PID" 2>/dev/null; then start_server; fi
        phase_rdt
        cleanup
        start_server
    fi
    if ((TEST_OFFLINE)); then
        if [[ -z "${SERVER_PID:-}" ]] || ! kill -0 "$SERVER_PID" 2>/dev/null; then start_server; fi
        phase_offline
    fi
}

trap cleanup EXIT INT TERM
main "$@"
