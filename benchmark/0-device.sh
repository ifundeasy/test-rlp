#!/usr/bin/env zsh

set -euo pipefail

SCRIPT_DIR=${0:a:h}
LOG_FILE="$SCRIPT_DIR/0-device.log"

ts() { date "+%Y-%m-%d %H:%M:%S%z"; }
section() { local name=$1; echo "== $name ==" | tee -a "$LOG_FILE"; }
kv() { local tag=$1; local key=$2; local val=$3; echo "[$tag] $key: $val" | tee -a "$LOG_FILE"; }

bytes_to_gb() { local b=$1; echo $(( b / 1024 / 1024 / 1024 ))GB; }
# Return GB with one decimal, e.g., 7.5GB
bytes_to_gb_1dp() {
  local b=$1
  awk -v bytes="$b" 'BEGIN { printf "%.1fGB", bytes/1024/1024/1024 }'
}

detect_os() {
  local os="$(uname -s)"
  case "$os" in
    Darwin)
      echo "macos"
      ;;
    Linux)
      echo "linux"
      ;;
    *)
      echo "unknown"
      ;;
  esac
}

hardware_info() {
  local platform=$(detect_os)
  section "Device"
  kv device "timestamp" "$(ts)"
  kv device "uname" "$(uname -a | tr -s ' ')"
  if [[ "$platform" == "macos" ]]; then
    # Format sw_vers to compact key=value;key=value
    local osv
    osv=$(sw_vers | awk -F':\t*' 'BEGIN{first=1} {gsub(/^ +| +$/,"",$1); gsub(/^ +| +$/,"",$2); printf "%s%s=%s", (first?"":";"), $1, $2; first=0 }')
    kv device "os" "$osv"
    local model=""
    if model=$(sysctl -n hw.model 2>/dev/null); then :; else
      model=$(system_profiler -detailLevel mini SPHardwareDataType 2>/dev/null | awk -F': ' '/Model Identifier/{print $2}')
    fi
    [[ -n "$model" ]] && kv device "model" "$model"
    local cpu_brand cores_p cores_l
    cpu_brand=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo "unknown")
    cores_p=$(sysctl -n hw.physicalcpu 2>/dev/null || echo "0")
    cores_l=$(sysctl -n hw.logicalcpu 2>/dev/null || echo "0")
    kv device "cpu" "$cpu_brand"
    kv device "cores_physical" "$cores_p"
    kv device "cores_logical" "$cores_l"
    local mem_bytes mem_gb
    mem_bytes=$(sysctl -n hw.memsize 2>/dev/null || echo "0")
    mem_gb=$(bytes_to_gb "$mem_bytes")
    kv device "memory" "$mem_gb (${mem_bytes}B)"
  else
    # linux
    if [[ -r /etc/os-release ]]; then
      kv device "os" "$(tr '\n' '; ' < /etc/os-release | sed 's/; $//' | sed -E 's/ +/ /g')"
    fi
    local model=""
    if [[ -r /sys/devices/virtual/dmi/id/product_name ]]; then
      model=$(cat /sys/devices/virtual/dmi/id/product_name)
    fi
    [[ -n "$model" ]] && kv device "model" "$model"
    local cpu_brand cores_l cores_p
    cpu_brand=$(awk -F': ' '/^model name/{print $2; exit}' /proc/cpuinfo 2>/dev/null || echo "unknown")
    cores_l=$(nproc 2>/dev/null || echo "0")
    cores_p=$(lscpu 2>/dev/null | awk -F': *' '/^Core\(s\) per socket/{c=$2} /^Socket\(s\)/{s=$2} END{if(c&&s)print c*s}' || echo "0")
    kv device "cpu" "$cpu_brand"
    kv device "cores_physical" "$cores_p"
    kv device "cores_logical" "$cores_l"
    local mem_total_kb
    mem_total_kb=$(awk '/MemTotal:/{print $2}' /proc/meminfo 2>/dev/null || echo "0")
    local mem_gb=$(bytes_to_gb $(( mem_total_kb * 1024 )))
    kv device "memory" "$mem_gb (${mem_total_kb}KB)"
  fi
  local disk_free disk_total
  disk_free=$(df -H / | awk 'NR==2{print $4}')
  disk_total=$(df -H / | awk 'NR==2{print $2}')
  kv device "disk_total" "$disk_total"
  kv device "disk_free" "$disk_free"
}

docker_info() {
  section "Docker"
  local dver_client dver_server compose_ver
  dver_client=$(docker version --format '{{.Client.Version}}' 2>/dev/null || echo "unknown")
  dver_server=$(docker version --format '{{.Server.Version}}' 2>/dev/null || echo "unknown")
  compose_ver=$(docker compose version --short 2>/dev/null || true)
  kv docker "version_client" "$dver_client"
  kv docker "version_server" "$dver_server"
  [[ -n "$compose_ver" ]] && kv docker "compose_version" "$compose_ver"

  local info
  info=$(docker info 2>/dev/null || true)
  local cpus mem mem_bytes swaplim
  # Prefer Go template for robustness
  local templ_vals
  templ_vals=$(docker info --format '{{.NCPU}}|{{.MemTotal}}' 2>/dev/null || echo "|")
  cpus=${templ_vals%%|*}
  mem_bytes=${templ_vals#*|}
  if [[ -z "$cpus" || "$cpus" == "|" ]]; then
    cpus=$(echo "$info" | awk -F': ' '/^CPUs/{print $2}' | head -n1)
  fi
  if [[ -z "$mem_bytes" || "$mem_bytes" == "|" ]]; then
    mem=$(echo "$info" | awk -F': ' '/^Total Memory/{print $2}' | head -n1)
  else
    # Convert bytes to GB string with one decimal to reflect Docker Desktop UI
    mem=$(bytes_to_gb_1dp "$mem_bytes")
  fi
  swaplim=$(echo "$info" | awk -F': ' '/^Total Swap|^Swap Limit/{print $2}' | head -n1)
  [[ -n "$cpus" ]] && kv docker "cpus" "$cpus"
  [[ -n "$mem" ]] && kv docker "memory" "$mem"
  if [[ -n "$swaplim" ]]; then
    kv docker "swap" "$swaplim"
  else
    # Fallback: host swap
    local platform=$(detect_os)
    if [[ "$platform" == "macos" ]]; then
      local sw_line=$(sysctl vm.swapusage 2>/dev/null | tr -s ' ')
      local sw=$(echo "$sw_line" | sed -E 's/.*used = ([0-9\.]+[KMGT]?B?), free = ([0-9\.]+[KMGT]?B?), total = ([0-9\.]+[KMGT]?B?).*/total=\3 used=\1 free=\2/')
      [[ -n "$sw" ]] && kv docker "swap_host" "$sw"
    else
      local sw_total=$(awk '/SwapTotal:/{print $2"KB"}' /proc/meminfo 2>/dev/null)
      [[ -n "$sw_total" ]] && kv docker "swap_host_total" "$sw_total"
    fi
  fi

  local runtime cgroup
  runtime=$(echo "$info" | awk -F': ' '/^Default Runtime/{print $2}' | head -n1)
  cgroup=$(echo "$info" | awk -F': ' '/^Cgroup Driver/{print $2}' | head -n1)
  [[ -n "$runtime" ]] && kv docker "default_runtime" "$runtime"
  [[ -n "$cgroup" ]] && kv docker "cgroup_driver" "$cgroup"
}

main() {
  : > "$LOG_FILE"
  hardware_info
  docker_info
  echo "" >> "$LOG_FILE"
  kv device "written" "$LOG_FILE"
}

main "$@"