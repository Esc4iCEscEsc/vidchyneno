#! /usr/bin/env bash

set -xe

DOMAINS=$(cat domains)
SUBNETS=()

for DOMAIN in $DOMAINS
do
  # Just the subnet we're on
  SUBNET=$(timeout 1 whois $(timeout 1 dig +short $DOMAIN) | grep "route:" | head -n 1 | tr -s ' ' | cut -d ' ' -f 2)
  SUBNETS+=($SUBNET)
  # Figure out the AS/s of the IP, then take all those subnets
  # AS=$(timeout 1 whois $(timeout 1 dig +short $DOMAIN) | grep "origin:" | tr -s ' ' | cut -d ' ' -f 2)
  # for A in $AS
  #   do
  #   PREFIXES=$(curl https://api.bgpview.io/asn/$A/prefixes | jq -r '.data.ipv4_prefixes[].prefix')
  #   for PREFIX in $PREFIXES
  #   do
  #     echo $PREFIX
  #     SUBNETS+=($PREFIX)
  #   done
  # done
done

echo "######"
printf -v joined '%s ' "${SUBNETS[@]}"
echo "${joined% }" > subnets.tmp
