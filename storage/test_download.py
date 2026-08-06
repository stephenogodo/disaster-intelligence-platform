from storage.download_state import (
    save_state,
    load_state,
    delete_state,
    DownloadState,
)

state = DownloadState(
    dataset="public_assistance",
    skip=462000,
    rows_downloaded=462000,
)

save_state(state)

loaded = load_state("public_assistance")

print(loaded)

delete_state("public_assistance")