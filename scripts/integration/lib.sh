#!/usr/bin/env bash

# Shared helpers for the MPUDP Linux namespace integration harness. Callers
# enable strict mode themselves so this file can also be sourced by teardown
# while recovering a partially written state directory.

MPUDP_IT_SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
MPUDP_IT_REPO_ROOT=$(CDPATH= cd -- "${MPUDP_IT_SCRIPT_DIR}/../.." && pwd -P)
readonly MPUDP_IT_SCRIPT_DIR MPUDP_IT_REPO_ROOT

mpudp_it_log() {
	printf '[mpudp-it] %s\n' "$*" >&2
}

mpudp_it_die() {
	mpudp_it_log "ERROR: $*"
	return 1
}

mpudp_it_validate_run_id() {
	local run_id=${1:-}
	if [[ ! ${run_id} =~ ^[a-z0-9][a-z0-9-]{0,47}$ ]]; then
		mpudp_it_die "invalid run ID; expected 1..48 lowercase letters, digits, or hyphens"
		return 1
	fi
}

mpudp_it_new_run_id() {
	local random_part
	if [[ -r /proc/sys/kernel/random/uuid ]]; then
		IFS= read -r random_part </proc/sys/kernel/random/uuid
		random_part=${random_part%%-*}
	else
		random_part=$(printf '%08x' "$RANDOM$RANDOM")
	fi
	printf '%s-%s-%s\n' "$(date -u +%Y%m%d%H%M%S)" "$$" "${random_part}"
}

mpudp_it_token_for() {
	local run_id=$1
	printf '%s' "${run_id}" | sha256sum | cut -c1-8
}

mpudp_it_new_owner_token() {
	local value
	if [[ -r /proc/sys/kernel/random/uuid ]]; then
		IFS= read -r value </proc/sys/kernel/random/uuid
		value=${value//-/}
		printf '%s\n' "${value}"
	else
		printf '%s:%s:%s\n' "$$" "${RANDOM}" "$(date -u +%s%N)" | sha256sum | cut -c1-32
	fi
}

mpudp_it_validate_owner_token() {
	local token=${1:-}
	if [[ ! ${token} =~ ^[0-9a-f]{32}$ ]]; then
		mpudp_it_die "invalid invocation owner token"
		return 1
	fi
}

mpudp_it_default_state_dir() {
	local run_id=$1
	printf '/tmp/mpudp-it-%s\n' "${run_id}"
}

mpudp_it_require_root() {
	if (( EUID != 0 )); then
		mpudp_it_die "root or equivalent CAP_NET_ADMIN/CAP_SYS_ADMIN is required"
		return 1
	fi
}

mpudp_it_require_commands() {
	local command_name missing=0
	for command_name in "$@"; do
		if ! command -v "${command_name}" >/dev/null 2>&1; then
			mpudp_it_log "missing command: ${command_name}"
			missing=1
		fi
	done
	if (( missing != 0 )); then
		mpudp_it_die "required integration commands are unavailable"
		return 1
	fi
}

# Complete one short kernel mutation even when the parent receives a handled
# signal. The caller keeps its mutation guard set, records ownership after a
# real zero exit, and only then acts on the pending signal.
mpudp_it_run_signal_shielded() {
	local command_pid status
	(
		trap '' HUP INT TERM
		exec "$@"
	) &
	command_pid=$!
	while :; do
		if wait "${command_pid}"; then status=0; else status=$?; fi
		if ! kill -0 "${command_pid}" 2>/dev/null; then
			return "${status}"
		fi
	done
}

mpudp_it_init_state() {
	local run_id=$1 state_dir=$2 owner_token=$3 token
	mpudp_it_validate_run_id "${run_id}" || return 1
	mpudp_it_validate_owner_token "${owner_token}" || return 1
	token=$(mpudp_it_token_for "${run_id}") || return 1
	if [[ -e ${state_dir} || -L ${state_dir} ]]; then
		mpudp_it_die "state path already exists: ${state_dir}"
		return 1
	fi
	(umask 077 && mkdir -- "${state_dir}") || return 1
	MPUDP_IT_STATE_CREATED=1
	printf 'mpudp integration state v1\n' >"${state_dir}/.mpudp-integration-state" || return 1
	printf '%s\n' "${run_id}" >"${state_dir}/run-id" || return 1
	printf '%s\n' "${token}" >"${state_dir}/token" || return 1
	printf '%s\n' "${MPUDP_IT_REPO_ROOT}" >"${state_dir}/repo-root" || return 1
	: >"${state_dir}/namespaces" || return 1
	: >"${state_dir}/host-links" || return 1
	: >"${state_dir}/pids" || return 1
	: >"${state_dir}/events.ndjson" || return 1
	printf '%s\n' "${owner_token}" >"${state_dir}/owner-token" || return 1
}

mpudp_it_load_state() {
	local state_dir=$1 expected_token owner_uid
	if [[ -z ${state_dir} || ! -d ${state_dir} || -L ${state_dir} ]]; then
		mpudp_it_die "invalid state directory: ${state_dir:-<empty>}"
		return 1
	fi
	if [[ ! -f ${state_dir}/.mpudp-integration-state || ! -f ${state_dir}/run-id || ! -f ${state_dir}/token || ! -f ${state_dir}/owner-token ]]; then
		mpudp_it_die "state directory is missing its MPUDP ownership marker: ${state_dir}"
		return 1
	fi
	state_dir=$(CDPATH= cd -- "${state_dir}" && pwd -P) || return 1
	owner_uid=$(stat -c '%u' -- "${state_dir}")
	if [[ ${owner_uid} != "${EUID}" ]]; then
		mpudp_it_die "state directory owner ${owner_uid} does not match current uid ${EUID}"
		return 1
	fi
	IFS= read -r MPUDP_IT_RUN_ID <"${state_dir}/run-id"
	IFS= read -r MPUDP_IT_TOKEN <"${state_dir}/token"
	IFS= read -r MPUDP_IT_OWNER_TOKEN <"${state_dir}/owner-token"
	mpudp_it_validate_run_id "${MPUDP_IT_RUN_ID}"
	mpudp_it_validate_owner_token "${MPUDP_IT_OWNER_TOKEN}"
	expected_token=$(mpudp_it_token_for "${MPUDP_IT_RUN_ID}")
	if [[ ${MPUDP_IT_TOKEN} != "${expected_token}" || ! ${MPUDP_IT_TOKEN} =~ ^[0-9a-f]{8}$ ]]; then
		mpudp_it_die "state token does not match run ID"
		return 1
	fi
	MPUDP_IT_STATE_DIR=${state_dir}
	MPUDP_IT_NS_PREFIX="mpudp-it-${MPUDP_IT_RUN_ID}"
	MPUDP_IT_TABLE4="mpu_${MPUDP_IT_TOKEN}_4"
	MPUDP_IT_TABLE6="mpu_${MPUDP_IT_TOKEN}_6"
	export MPUDP_IT_RUN_ID MPUDP_IT_TOKEN MPUDP_IT_OWNER_TOKEN MPUDP_IT_STATE_DIR MPUDP_IT_NS_PREFIX MPUDP_IT_TABLE4 MPUDP_IT_TABLE6
}

mpudp_it_assert_state_child_path() {
	local path=${1:-} parent base resolved_parent
	if [[ -z ${MPUDP_IT_STATE_DIR:-} || -z ${path} || ${path} != /* ]]; then
		mpudp_it_die "state output must be an absolute direct child path: ${path:-<empty>}"
		return 1
	fi
	parent=${path%/*}
	base=${path##*/}
	if [[ -z ${base} || ${base} == . || ${base} == .. ]]; then
		mpudp_it_die "invalid state output basename: ${base:-<empty>}"
		return 1
	fi
	resolved_parent=$(CDPATH= cd -- "${parent}" 2>/dev/null && pwd -P) || {
		mpudp_it_die "state output parent does not exist: ${parent}"
		return 1
	}
	if [[ ${resolved_parent} != "${MPUDP_IT_STATE_DIR}" || ${path} != "${MPUDP_IT_STATE_DIR}/${base}" ]]; then
		mpudp_it_die "state output must be a canonical direct child: ${path}"
		return 1
	fi
}

mpudp_it_ns() {
	local role=$1
	case ${role} in
		alice | bob | t1 | t2 | t3 | t4 | t5) ;;
		*) mpudp_it_die "invalid namespace role: ${role}"; return 1 ;;
	esac
	printf '%s-%s\n' "${MPUDP_IT_NS_PREFIX}" "${role}"
}

mpudp_it_assert_owned_namespace() {
	local namespace=$1 role
	for role in alice t1 t2 t3 t4 t5 bob; do
		if [[ ${namespace} == "$(mpudp_it_ns "${role}")" ]]; then
			return 0
		fi
	done
	mpudp_it_die "refusing non-owned namespace: ${namespace}"
}

mpudp_it_host_link_name() {
	local suffix=$1
	if [[ ! ${suffix} =~ ^[a-z0-9]{1,5}$ ]]; then
		mpudp_it_die "invalid host-link suffix: ${suffix}"
		return 1
	fi
	printf 'mi%s%s\n' "${MPUDP_IT_TOKEN}" "${suffix}"
}

mpudp_it_assert_owned_host_link() {
	local link=$1
	if [[ ! ${link} =~ ^mi${MPUDP_IT_TOKEN}[a-z0-9]{1,5}$ || ${#link} -gt 15 ]]; then
		mpudp_it_die "refusing non-owned host link: ${link}"
		return 1
	fi
}

mpudp_it_record_namespace() {
	local namespace=$1
	mpudp_it_assert_owned_namespace "${namespace}"
	printf '%s\n' "${namespace}" >>"${MPUDP_IT_STATE_DIR}/namespaces"
}

mpudp_it_record_host_link() {
	local link=$1
	mpudp_it_assert_owned_host_link "${link}"
	printf '%s\n' "${link}" >>"${MPUDP_IT_STATE_DIR}/host-links"
}

mpudp_it_process_start_time() {
	local pid=$1 stat rest
	if [[ ! -r /proc/${pid}/stat ]]; then
		return 1
	fi
	IFS= read -r stat <"/proc/${pid}/stat"
	rest=${stat##*) }
	# starttime is field 22 overall, or field 20 after removing pid and comm.
	set -- ${rest}
	printf '%s\n' "${20}"
}

mpudp_it_record_pid() {
	local pid=$1 start_time
	if [[ ! ${pid} =~ ^[1-9][0-9]*$ ]]; then
		mpudp_it_die "invalid process ID: ${pid}"
		return 1
	fi
	start_time=$(mpudp_it_process_start_time "${pid}") || {
		mpudp_it_die "process ${pid} exited before it could be recorded"
		return 1
	}
	printf '%s %s\n' "${pid}" "${start_time}" >>"${MPUDP_IT_STATE_DIR}/pids"
}

mpudp_it_process_is_owned() {
	local pid=$1 expected_start=$2 actual_start
	actual_start=$(mpudp_it_process_start_time "${pid}") || return 1
	[[ ${actual_start} == "${expected_start}" ]] || return 1
	mpudp_it_process_matches_run "${pid}"
}

mpudp_it_process_matches_run() {
	local pid=$1 argument expect_run_id=0 found_run_id=0 found_helper=0
	[[ -r /proc/${pid}/cmdline ]] || return 1
	while IFS= read -r -d '' argument; do
		if (( expect_run_id != 0 )); then
			if [[ ${argument} == "${MPUDP_IT_RUN_ID}" ]]; then found_run_id=1; fi
			expect_run_id=0
		elif [[ ${argument} == --run-id ]]; then
			expect_run_id=1
		elif [[ ${argument} == "--run-id=${MPUDP_IT_RUN_ID}" ]]; then
			found_run_id=1
		fi
		case ${argument} in
			*/netprobe | */peerprobe | */capture-fragments | */capture-udp) found_helper=1 ;;
		esac
	done <"/proc/${pid}/cmdline"
	(( found_run_id != 0 && found_helper != 0 ))
}

mpudp_it_path_check() {
	local path=$1
	if [[ ! ${path} =~ ^[1-5]$ ]]; then
		mpudp_it_die "path must be 1..5, got ${path}"
		return 1
	fi
}

mpudp_it_family_check() {
	local family=$1
	if [[ ${family} != 4 && ${family} != 6 ]]; then
		mpudp_it_die "family must be 4 or 6, got ${family}"
		return 1
	fi
}

mpudp_it_alice_if() { printf 'a%sp%s\n' "$1" "$2"; }
mpudp_it_bob_if() { printf 'b%sp%s\n' "$1" "$2"; }
mpudp_it_t_in_if() { printf 'in%s\n' "$1"; }
mpudp_it_t_out_if() { printf 'out%s\n' "$1"; }

mpudp_it_t_ingress_addr() {
	local family=$1 path=$2
	if [[ ${family} == 4 ]]; then
		printf '10.101.%s.1\n' "${path}"
	else
		printf 'fd42:101:%s::1\n' "${path}"
	fi
}

mpudp_it_alice_addr() {
	local family=$1 path=$2
	if [[ ${family} == 4 ]]; then
		printf '10.101.%s.2\n' "${path}"
	else
		printf 'fd42:101:%s::2\n' "${path}"
	fi
}

mpudp_it_t_egress_addr() {
	local family=$1 path=$2
	if [[ ${family} == 4 ]]; then
		printf '10.102.%s.1\n' "${path}"
	else
		printf 'fd42:102:%s::1\n' "${path}"
	fi
}

mpudp_it_bob_addr() {
	local family=$1 path=$2
	if [[ ${family} == 4 ]]; then
		printf '10.102.%s.2\n' "${path}"
	else
		printf 'fd42:102:%s::2\n' "${path}"
	fi
}

mpudp_it_prefix_length() {
	if [[ $1 == 4 ]]; then printf '30\n'; else printf '64\n'; fi
}

mpudp_it_target() {
	local family=$1 path=$2 address
	address=$(mpudp_it_t_ingress_addr "${family}" "${path}")
	if [[ ${family} == 4 ]]; then
		printf '%s:4000\n' "${address}"
	else
		printf '[%s]:4000\n' "${address}"
	fi
}

mpudp_it_join_targets() {
	local family=$1 path result=''
	for path in 1 2 3 4 5; do
		if [[ -n ${result} ]]; then result+=','; fi
		result+=$(mpudp_it_target "${family}" "${path}")
	done
	printf '%s\n' "${result}"
}

mpudp_it_safe_remove_state() {
	local state_dir=$1
	if [[ -z ${state_dir} || ${state_dir} == / || ! -d ${state_dir} || -L ${state_dir} ]]; then
		mpudp_it_die "refusing unsafe state removal: ${state_dir:-<empty>}"
		return 1
	fi
	if [[ ! -f ${state_dir}/.mpudp-integration-state ]]; then
		mpudp_it_die "refusing unmarked state removal: ${state_dir}"
		return 1
	fi
	find "${state_dir}" -xdev -depth -delete
}
