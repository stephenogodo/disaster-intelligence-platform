from storage.parquet_repository import ParquetRepository

repo = ParquetRepository()

print("Configured datasets:")
print(repo.datasets())

print()

for dataset in repo.datasets():
    print(f"{dataset}")
    print(" Path :", repo.dataset_path(dataset))
    print(" Exists:", repo.exists(dataset))
    print()