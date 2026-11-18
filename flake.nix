{
  description = "The Codex Harness Nexus: what each Codex session is doing, told by subscription.";
  inputs.base.url = "github:LiGoldragon/build-base";
  inputs.ethos.url = "github:LiGoldragon/ethos-zero";
  outputs =
    { self, base, ethos }:
    let
      lib = base.inputs.nixpkgs.lib;
      cargoNix = ./Cargo.nix;
      # What the pair launcher starts both processes with: the admission
      # every frame between them is read and written within, and the text
      # budget the translator starts with, its own setting on the meta socket
      # thereafter.
      pairing = {
        frameDeadlineMilliseconds = 70000;
        frameCapacityBytes = 8388608;
        retryDelayMilliseconds = 25;
        textNodeLimit = 1048576;
        textDepthLimit = 64;
      };
      workspaces = lib.genAttrs base.lib.systems (
        system:
        let
          pkgs = base.lib.contexts.${system}.pkgs;
          composed = base.lib.workspace.${system} { inherit cargoNix; };
          # A member built with the features its binaries are written for:
          # the daemon's plain, the translator's with text.
          daemon = composed.binaries "codex-harness-nexus-daemon" [ ];
          built = composed.binaries "codex-harness-translator" [ "default" "text" ];
          translator = pkgs.runCommand "codex-harness-translator" { } ''
            mkdir -p "$out/bin"
            cp ${built}/bin/codex-harness-translator "$out/bin/codex-harness-translator"
          '';
          cli = pkgs.runCommand "codex-harness-nexus-cli" { } ''
            mkdir -p "$out/bin" "$out/libexec"
            for executable in codex-harness codex-harness-meta; do
              cp ${built}/bin/$executable "$out/bin/$executable"
            done
            cp ${built}/bin/codex-harness-pair "$out/libexec/codex-harness-pair-launcher"
          '';
          pair = pkgs.writeShellScriptBin "codex-harness-pair" ''
            if [[ $# -ne 1 ]]; then
              echo "usage: codex-harness-pair '<Place>'" >&2
              exit 64
            fi
            exec ${cli}/libexec/codex-harness-pair-launcher "$1" \
              ${daemon}/bin/codex-harness-nexus \
              ${translator}/bin/codex-harness-translator \
              ${toString pairing.frameDeadlineMilliseconds} \
              ${toString pairing.frameCapacityBytes} \
              ${toString pairing.retryDelayMilliseconds} \
              ${toString pairing.textNodeLimit} \
              ${toString pairing.textDepthLimit}
          '';
          default = pkgs.symlinkJoin {
            name = "codex-harness-nexus";
            paths = [ daemon translator cli pair ];
          };
        in
        {
          inherit daemon translator cli pair default;
          # The member's own package is its binaries with text, never with
          # default features alone.
          codex-harness-translator = built;
        }
      );
      workspace = base.lib.workspaceOutputs {
        inherit cargoNix;
        # The translator's lanes, its process and its CLIs are written for
        # text, so its tests are built with it.
        memberChecks =
          { check, ... }:
          {
            codex-harness-translator = check "codex-harness-translator" [ "default" "text" ];
          };
        extraChecks =
          { pkgs, toolchain, ... }:
          {
            fmt = pkgs.runCommand "codex-harness-nexus-fmt" { nativeBuildInputs = [ toolchain ]; } ''
              cd ${./crates}
              rustfmt --edition 2024 --check \
                codex-harness-nexus/src/{lib,folded,naming,nexus,store,views,watching}.rs \
                codex-harness-translator/src/{lib,emitting,rollout,translating}.rs \
                codex-harness-translator/src/bin/{codex-harness,codex-harness-meta,codex-harness-translator,codex-harness-pair}.rs \
                codex-harness-nexus-daemon/src/{lib,bootstrap,emitting,main,serving,waking}.rs \
                codex-harness-nexus-daemon/tests/bootstrap_order.rs
              touch "$out"
            '';
            generated = pkgs.runCommand "codex-harness-nexus-generated" {
              nativeBuildInputs = [ ethos.packages.${pkgs.stdenv.hostPlatform.system}.ethos-zero ];
            } ''
              for file in bootstrap emission folding memory operation runtime; do
                ethos-zero "Generate.{ ${./crates/codex-harness-nexus/ethos}/$file.ethos $PWD/generated }"
                diff -u ${./crates/codex-harness-nexus/src/generated}/$file.rs generated/$file.rs
              done
              ethos-zero "Generate.{ ${./crates/codex-harness-translator/ethos}/rollout.ethos $PWD/generated }"
              diff -u ${./crates/codex-harness-translator/src/generated}/rollout.rs generated/rollout.rs
              touch "$out"
            '';
          };
      };
    in
    {
      packages = lib.mapAttrs (system: packages: workspace.packages.${system} // packages) workspaces;
      inherit (workspace) checks devShells;
    };
}
