#!/bin/bash

function start() {
  run_in_env python -m scheduling "$@"
}

function gendoc() {
  cd docs || exit 1
  run_in_env make html
  xdg-open "build/html/index.html"
}

function run_in_env() {
  poetry install
  poetry run "$@"
}

function element_in() {
  local e match="$1"
  shift
  for e; do [[ "$e" == "$match" ]] && return 0; done
  return 1
}

source "$HOME/.poetry/env"
COMMANDS=("start" "gendoc")
if element_in "$1" "${COMMANDS[@]}"; then
  COMMAND="$1"
  shift

  "$COMMAND" "$@"
else
  echo "Usage: $0 {$(
    IFS='|'
    echo "${COMMANDS[*]}"
  )}"
fi
