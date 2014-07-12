#!/bin/bash
set -e
git clone https://github.com/tel/riemann-hs.git
cabal sandbox init
cabal sandbox add-source riemann-hs
cabal install --dependencies-only
cabal build
