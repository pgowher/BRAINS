#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
mkdir -p .tools

fetch_verified() {
    local url="$1" checksum="$2" path="$3" partial="${3}.partial"
    curl -fsSL --retry 2 "$url" -o "$partial"
    printf '%s  %s\n' "$checksum" "$partial" | sha256sum -c -
    mv -- "$partial" "$path"
}

if [[ ! -x .tools/besu-26.10.2/bin/besu ]]; then
    fetch_verified \
        https://github.com/besu-eth/besu/releases/download/26.10.2/besu-26.10.2.tar.gz \
        8cc633e19cb23cd38b5d6ad988b725c8533389a773aea65782fd16d37946825b .tools/besu.tar.gz
    tar -xzf .tools/besu.tar.gz -C .tools
    rm -- .tools/besu.tar.gz
fi
if [[ ! -x .tools/solc ]]; then
    fetch_verified \
        https://binaries.soliditylang.org/linux-amd64/solc-linux-amd64-v0.8.35+commit.47b9dedd \
        fa8ac9a32d301ad023a36ee5a29f8e291fe3200c60244e43c142539e82a617f4 .tools/solc
    chmod +x .tools/solc
fi
python3 -m venv .tools/venv
.tools/venv/bin/python -m pip --disable-pip-version-check install -r requirements.txt
.tools/besu-26.10.2/bin/besu --version
.tools/solc --version
