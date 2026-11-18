{
  description = "The Codex Harness Nexus: what each Codex session is doing, told by subscription.";
  inputs.base.url = "github:LiGoldragon/build-base";
  inputs.ethos.url = "github:LiGoldragon/ethos-zero";
  outputs = { self, base, ethos }: base.lib.workspaceOutputs {
    cargoNix = ./Cargo.nix;
    extraChecks = { pkgs, toolchain, ... }: {
      fmt = pkgs.runCommand "codex-harness-nexus-fmt" { nativeBuildInputs = [ toolchain ]; } ''
        cd ${./crates}
        rustfmt --edition 2024 --check \
          codex-harness-nexus/src/{lib,folded,nexus,store,views,watching}.rs \
          codex-harness-translator/src/{lib,emitting,rollout}.rs
        touch "$out"
      '';
      generated = pkgs.runCommand "codex-harness-nexus-generated" {
        nativeBuildInputs = [ ethos.packages.${pkgs.stdenv.hostPlatform.system}.ethos-zero ];
      } ''
        for file in emission folding memory operation runtime; do
          ethos-zero "Generate.{ ${./crates/codex-harness-nexus/ethos}/$file.ethos $PWD/generated }"
          diff -u ${./crates/codex-harness-nexus/src/generated}/$file.rs generated/$file.rs
        done
        ethos-zero "Generate.{ ${./crates/codex-harness-translator/ethos}/rollout.ethos $PWD/generated }"
        diff -u ${./crates/codex-harness-translator/src/generated}/rollout.rs generated/rollout.rs
        touch "$out"
      '';
    };
  };
}
