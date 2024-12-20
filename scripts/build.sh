#!/usr/bin/env bash
set -e

if [ ! -d node_modules ]; then
	echo "[B] Run 'yarn' first"
	exit 1
fi

rm -rf lib/*
node scripts/version.js > lib/version.json
node scripts/assemble_lua.js > lib/lua.json
cp src/*.js lib/
