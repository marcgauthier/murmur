#!/usr/bin/env bash
set -euo pipefail
client_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
go_os="$(go env GOOS)"
go_arch="$(go env GOARCH)"
case "$go_os/$go_arch" in
  linux/amd64) rust_target=x86_64-unknown-linux-gnu ;;
  linux/arm64) rust_target=aarch64-unknown-linux-gnu ;;
  darwin/amd64) rust_target=x86_64-apple-darwin ;;
  darwin/arm64) rust_target=aarch64-apple-darwin ;;
  windows/amd64) rust_target=x86_64-pc-windows-gnu ;;
  *) echo "Unsupported native target: $go_os/$go_arch" >&2; exit 1 ;;
esac
profile=debug
cargo_profile=()
if [[ "${1:-}" == --release ]]; then profile=release; cargo_profile=(--release); shift; fi
rustup target add "$rust_target"
mkdir -p "$client_dir/native/lib/$go_os-$go_arch"
build_log="$(mktemp)"
trap 'rm -f "$build_log"' EXIT
cargo rustc --locked --manifest-path "$client_dir/native/Cargo.toml" \
  --target "$rust_target" --lib "${cargo_profile[@]}" -- --print native-static-libs 2>&1 | tee "$build_log"
cp "$client_dir/native/target/$rust_target/$profile/libmurmur_go.a" "$client_dir/native/lib/$go_os-$go_arch/"
native_flags="$(sed -n 's/^note: native-static-libs: //p' "$build_log" | tail -1)"
printf '%s\n' "$native_flags" > "$client_dir/native/lib/$go_os-$go_arch/native-static-libs.txt"
go run "$client_dir/internal/buildstamp/main.go" "$client_dir/native/lib/$go_os-$go_arch/libmurmur_go.a" "$client_dir/native_build_stamp.go"
echo "Native library built for $go_os/$go_arch ($profile)."
if [[ $# -gt 0 ]]; then
  if [[ "$1" == test ]]; then
    cargo build --locked --manifest-path "$client_dir/native/Cargo.toml" --target "$rust_target" "${cargo_profile[@]}" --example peer_fixture
    export MURMUR_RUST_FIXTURE="$client_dir/native/target/$rust_target/$profile/examples/peer_fixture"
  fi
  cd "$client_dir"
  CGO_ENABLED=1 CGO_LDFLAGS="${CGO_LDFLAGS:-} $native_flags" go "$@"
fi
