{
  description = "Data Engineering Stack - Zapier Insights API";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = inputs @ {
    self,
    nixpkgs,
    flake-utils,
    ...
  }:
    flake-utils.lib.eachDefaultSystem (
      system: let
        pkgs = nixpkgs.legacyPackages.${system};
      in {
        devShells.default = pkgs.mkShellNoCC {
          packages = with pkgs; [
            python311
            uv
            # Database tools
            postgresql
            # Geospatial support (if needed)
            gdal
          ];

          shellHook = ''
            echo "🚀 Zapier Insights API Development Environment"
            echo "📦 Python: $(python --version)"
            echo "⚡ uv: $(uv --version)"
            echo ""
            echo "Quick start:"
            echo "  make install  # Install dependencies"
            echo "  make run      # Start development server"
            echo "  make test     # Run tests"
            echo ""
          '';
        };
      }
    );
}
