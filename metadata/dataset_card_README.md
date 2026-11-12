---
language:
- en
license: cc0-1.0
size_categories:
- 1M<n<10M
task_categories:
- text-generation
- text-classification
pretty_name: arXiv Metadata Dataset
configs:
- config_name: default
  data_files:
  - split: train
    path: data/train-*
dataset_info:
  features:
  - name: id
    dtype: string
  - name: submitter
    dtype: string
  - name: authors
    dtype: string
  - name: title
    dtype: string
  - name: comments
    dtype: string
  - name: journal-ref
    dtype: string
  - name: doi
    dtype: string
  - name: report-no
    dtype: string
  - name: categories
    dtype: string
  - name: license
    dtype: string
  - name: abstract
    dtype: string
  - name: versions
    list:
    - name: version
      dtype: string
    - name: created
      dtype: string
  - name: update_date
    dtype: timestamp[s]
  - name: authors_parsed
    list:
      list: string
  splits:
  - name: train
    num_bytes: 5088171848
    num_examples: 3224635
  download_size: 2700966734
  dataset_size: 5088171848
tags:
- arxiv
- science
---
# Dataset Card for "arxiv-metadata-oai-snapshot"

[More Information needed](https://github.com/huggingface/datasets/blob/main/CONTRIBUTING.md#how-to-contribute-to-the-dataset-cards)

This is a mirror of the metadata portion of the arXiv [dataset](https://www.kaggle.com/datasets/Cornell-University/arxiv/versions/147). 

The sync will take place weekly so may fall behind the original datasets slightly if there are more regular updates to the source dataset. 


## Metadata

This dataset is a mirror of the original ArXiv data. This dataset contains an entry for each paper, containing:

- id: ArXiv ID (can be used to access the paper, see below)
- submitter: Who submitted the paper
- authors: Authors of the paper
- title: Title of the paper
- comments: Additional info, such as number of pages and figures
- journal-ref: Information about the journal the paper was published in
- doi: [https://www.doi.org](Digital Object Identifier)
- abstract: The abstract of the paper
- categories: Categories / tags in the ArXiv system
- versions: A version history

You can access each paper directly on ArXiv using these links:

- `https://arxiv.org/abs/{id}`: Page for this paper including its abstract and further links
- `https://arxiv.org/pdf/{id}`: Direct link to download the PDF

