{
  description = "The Codex Harness Nexus, built on the process manager and the translator.";

  inputs.base.url = "github:LiGoldragon/build-base";

  outputs = { self, base }: base.lib.workspaceOutputs { cargoNix = ./Cargo.nix; };
}
