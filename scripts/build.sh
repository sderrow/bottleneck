#!/usr/bin/env bash
set -e

nvm use

if [ ! -d node_modules ]; then
	echo "[B] Run 'yarn' first"
	exit 1
fi

clean() {
  rm -rf lib/*
  node scripts/version.js > lib/version.json
  node scripts/assemble_lua.js > lib/lua.json
}

makeLib() {
  echo '[B] Moving Bottleneck over to lib...'
  cp src/*.js lib/
}

makeTypings() {
  echo '[B] Testing TS typings...'
  yarn tsc --noEmit --strict test.ts
}

if [ "$1" = 'dev' ]; then
  clean
  makeLib
else
  clean
  makeLib
  makeTypings
fi


echo '[B] Done!'
