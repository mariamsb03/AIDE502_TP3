from datasets import load_dataset

dataset = load_dataset("wikitext", "wikitext-103-raw-v1")

# Inspect a sample
print(dataset["train"][0])

dataset.save_to_disk("wikitext_dataset")
