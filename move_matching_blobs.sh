#!/bin/bash

STORAGE_ACCOUNT="chicagobistorage"
CONTAINER="silver"
TARGET_FOLDER="clean_tnp_trips"
SAS_TOKEN="sv=2024-11-04&ss=bfqt&srt=co&sp=rwdlacupiytfx&se=2025-12-04T23:57:45Z&st=2025-11-12T15:42:45Z&spr=https&sig=8tJTEzqXCztYnFKjKBJ8BbssbQEdOhwoycyK9KTPpos%3D"

DRY_RUN=0  # set to 1 for dry run, 0 for actual copy/delete

while IFS= read -r raw_name
do
    # Remove quotes, spaces, commas
    blob_name=$(echo "$raw_name" | sed 's/[ ",]//g')

    # Skip empty after trimming
    if [[ -z "$blob_name" ]]; then
        continue
    fi

    echo "Would move: $blob_name -> $TARGET_FOLDER/${blob_name##*/}"

    if [[ $DRY_RUN -eq 0 ]]; then
        azcopy copy \
            "https://$STORAGE_ACCOUNT.blob.core.windows.net/$CONTAINER/$blob_name?$SAS_TOKEN" \
            "https://$STORAGE_ACCOUNT.blob.core.windows.net/$CONTAINER/$TARGET_FOLDER/${blob_name##*/}?$SAS_TOKEN" \
            --overwrite=true --output-level=quiet < /dev/null

        if [[ $? -eq 0 ]]; then
            az storage blob delete \
                --account-name "$STORAGE_ACCOUNT" \
                --container-name "$CONTAINER" \
                --name "$blob_name" \
                --sas-token "$SAS_TOKEN"
            echo "Deleted: $blob_name"
        else
            echo "Copy failed for $blob_name, not deleting."
        fi
    fi
done < files_tnp_list.csv
