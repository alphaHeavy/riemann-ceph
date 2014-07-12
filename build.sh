#!/bin/bash
set -e
git clone https://github.com/alphaHeavy/riemann-hs.git -b protobuf-0.2
cabal sandbox init
cabal sandbox add-source riemann-hs
cabal install --dependencies-only
cabal build
