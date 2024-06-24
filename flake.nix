{
  description = "Build a cargo project without extra checks";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

    crane = {
      url = "github:ipetkov/crane";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, crane, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (localSystem:
      let
        pkgs = import nixpkgs {
          system = localSystem;
        };

        craneLib = crane.lib.${localSystem};

        oh-no = craneLib.buildPackage {
          src = craneLib.cleanCargoSource (craneLib.path ./.);
          strictDeps = true;

          nativeBuildInputs = [ pkgs.llvmPackages.clang ];

          buildInputs = [
            pkgs.llvmPackages.clang
            pkgs.llvmPackages.libclang
            # Add additional build inputs here
          ] ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
            # Additional darwin specific inputs can be set here
            pkgs.libiconv
          ];

          # Additional environment variables can be set directly
          LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
        };
      in
      {
        checks = {
          inherit oh-no;
        };

        packages.default = oh-no;

        apps.default = flake-utils.lib.mkApp {
          drv = oh-no;
        };

        devShell = craneLib.devShell {
          # Inherit inputs from checks.
          checks = self.checks.${localSystem};

          # Additional dev-shell environment variables can be set directly

          # Extra inputs can be added here; cargo and rustc are provided by default.
          packages = [
            pkgs.rustfmt
            pkgs.just
            pkgs.rust-analyzer
          ];

        };
      });
}
