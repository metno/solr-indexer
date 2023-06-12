#!/bin/bash
set -x
ENV="-staging"
NS="no.met.staging"
UUIDS="$NS:64db6102-14ce-41e9-b93b-61dbb2cb8b4e
$NS:bc82c179-144e-415a-8dd2-64d3569a8d50
$NS:da280021-13d8-425e-9783-64911d772397
$NS:d42548cc-337f-4005-91f4-a5dc306244b0
$NS:e1d0863d-71d3-4b9f-bb17-2af42d4956e7
$NS:6827f045-36c1-4678-a0bf-d91b41f8eefb
$NS:c7f8731b-5cfe-4cb5-ac57-168a19a2957b
$NS:f6cbb81c-1ce1-4080-b242-819e59cee78d
$NS:8e5ec6e8-f0ac-47cc-a869-44373c204848"
for uuid in $UUIDS
 do
  echo $uuid
  indexdata -c etc/senda$ENV.yml -parent "$uuid"
done
