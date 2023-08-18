{
  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  };
  outputs = { self, nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
      in
      with pkgs;
      {
        devShell = mkShell {
          buildInputs = [
            python311
            python311Packages.discordpy
            python311Packages.python-dotenv
            python311Packages.jinja2
            discordchatexporter-cli
          ];
        };
      }
    );
}
