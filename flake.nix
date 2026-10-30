{
  description = "Declarative schema management tool for PostgreSQL";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs =
    { self, nixpkgs }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
    in
    {
      packages = forAllSystems (pkgs: {
        default = pkgs.buildGo127Module rec {
          pname = "pistachio";
          version = self.shortRev or self.dirtyShortRev or "dev";
          src = self;
          # Update this whenever go.mod or go.sum changes. Set it to
          # pkgs.lib.fakeHash and run nix build to get the new hash.
          vendorHash = "sha256-AjxZLxKSOdU+sB1NW9UzrEN01vd9YFucvJYg4vw1XSU=";
          subPackages = [ "cmd/pista" ];
          ldflags = [ "-X main.version=${version}" ];
          # The tests need PostgreSQL.
          doCheck = false;
          meta = {
            description = "Declarative schema management tool for PostgreSQL";
            homepage = "https://github.com/winebarrel/pistachio";
            license = pkgs.lib.licenses.mit;
            mainProgram = "pista";
          };
        };
      });
    };
}
