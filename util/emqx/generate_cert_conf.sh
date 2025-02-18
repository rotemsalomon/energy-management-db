#!/bin/bash

# Usage: ./generate_cert_conf.sh <common_name> <private_key> [--san <ip1,ip2,...>]

COMMON_NAME="$1"
PRIVATE_KEY="$2"
SAN_FLAG=$3
SAN_IPS=$4  # Optional, comma-separated list of IP addresses for SAN

# Validate input arguments
if [[ -z "$COMMON_NAME" || -z "$PRIVATE_KEY" ]]; then
  echo "Usage: $0 \"<common_name>\" \"<private_key>\" [--san <ip1,ip2,...>]"
  exit 1
fi

# Escape special characters in variables for use in sed
COMMON_NAME_ESCAPED=$(printf '%s\n' "$COMMON_NAME" | sed 's/[\/&]/\\&/g')
PRIVATE_KEY_ESCAPED=$(printf '%s\n' "$PRIVATE_KEY" | sed 's/[\/&]/\\&/g')

TEMPLATE_FILE="template_cert.conf"
OUTPUT_FILE="${COMMON_NAME}_cert.conf"

# Prepare SAN section if the flag is provided
SAN_SECTION=""
if [[ "$SAN_FLAG" == "--san" && -n "$SAN_IPS" ]]; then
  IFS=',' read -r -a SAN_ARRAY <<< "$SAN_IPS"
  SAN_SECTION="[ my_subject_alt_names ]\n"
  for i in "${!SAN_ARRAY[@]}"; do
    SAN_SECTION+="IP.$((i+1)) = ${SAN_ARRAY[i]}\n"
  done
  SAN_EXTENSION="subjectAltName = @my_subject_alt_names"
else
  SAN_EXTENSION=""
fi

# Read template and modify it based on input
sed -e "s/{{COMMON_NAME}}/$COMMON_NAME_ESCAPED/g" \
    -e "s/{{PRIVATE_KEY}}/$PRIVATE_KEY_ESCAPED/g" \
    -e "s/{{SAN_SECTION}}/$SAN_SECTION/g" \
    -e "s/{{SAN_EXTENSION}}/$SAN_EXTENSION/g" \
    "$TEMPLATE_FILE" > "$OUTPUT_FILE"

# Print success message
echo "Generated certificate configuration file with the following settings:"
echo "  Configuration file: $OUTPUT_FILE"
echo "  Common Name: $COMMON_NAME"
echo "  Private Key File: $PRIVATE_KEY"
if [[ -n "$SAN_SECTION" ]]; then
  echo "  Subject Alternative Names: $SAN_IPS"
else
  echo "  No Subject Alternative Names provided."
fi