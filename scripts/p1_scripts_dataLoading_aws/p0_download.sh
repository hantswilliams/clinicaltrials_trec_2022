#!/bin/bash

output_folder="trial_data"
mkdir -p "$output_folder"

urls=(
  "https://www.trec-cds.org/2023_data/ClinicalTrials.2023-05-08.trials0.zip"
  "https://www.trec-cds.org/2023_data/ClinicalTrials.2023-05-08.trials1.zip"
  "https://www.trec-cds.org/2023_data/ClinicalTrials.2023-05-08.trials2.zip"
  "https://www.trec-cds.org/2023_data/ClinicalTrials.2023-05-08.trials3.zip"
  "https://www.trec-cds.org/2023_data/ClinicalTrials.2023-05-08.trials4.zip"
  "https://www.trec-cds.org/2023_data/ClinicalTrials.2023-05-08.trials5.zip"
)

for url in "${urls[@]}"; do
  output_file="$output_folder/$(basename "$url")"
  wget "$url" -O "$output_file"
done