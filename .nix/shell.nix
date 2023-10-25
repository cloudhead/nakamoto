let
  nixpkgs = import <nixpkgs> {};
in
with nixpkgs; pkgs.mkShell {
  buildInputs = [
    libusb1
    pkg-config
    rustup
  ];
}
