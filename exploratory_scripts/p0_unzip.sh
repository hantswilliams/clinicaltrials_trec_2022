#!/bin/bash

input_folder="trial_data"

cd "$input_folder"

for file in *.zip; do
  unzip "$file"
done