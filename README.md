# MCI2026_Task2_Kelompok30

| Name           | NRP        |
| --- | --- |
| Muhammad Zahran Rizki Primanda            | 5025241107        |
| Severinus Fabian Tanuwidjaja            | 5025241110        |


## Pengenalan Dataset
Data diambil dari http://96.9.212.102:8000/orders yang berisikan file JSON yang berisikan rincian belanja dari 100 data pemesanan terakhir dengan struktur sebagai berikut:
```
JSON Response
        │
        ├── total_orders
        │     └── 100
        │
        └── orders[]
              │
              ├── order
              │    │
              │    ├── order_id
              │    ├── user_id
              │    ├── order_number
              │    ├── order_dow
              │    ├── order_hour_of_day
              │    ├── days_since_prior_order
              │    ├── eval_set
              │    │
              │    └── products[]
              │          │
              │          ├── product
              │          │    ├── product_id
              │          │    ├── product_name
              │          │    ├── aisle_id
              │          │    ├── aisle
              │          │    ├── department_id
              │          │    ├── department
              │          │    ├── add_to_cart_order
              │          │    └── reordered
              │          │
              │          ├── product
              │          └── ...
              │
              ├── order
              └── ...
```
<img width="858" height="884" alt="image" src="https://github.com/user-attachments/assets/14f645d7-2b4d-4039-ac5b-99bde3b58d9e" />

## Pengenalan Struktur Folder dan File
```
Folder/
│
├── dags/
│   │
│   ├── orders_pipeline.py
│   │
│   ├── scripts/
│   │   │
│   │   ├── fetch_orders.py
│   │   │
│   │   ├── process_orders_spark.py
│   │   │  
│   │   │
│   │   └── process_user_behavior.py
│   │
│   └── __pycache__/
│
├── data_lake/
│   │
│   └── orders/
│       │
│       ├── orders_20260517_....
│       ├── orders_20260517_....
│       └── ...
│
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── .gitignore
```

Dalam pengerjaan ini, total akan ada 1 folder utama yang menaungi keseluruhan file dan folder lain, 5 sub folder (dags, data_lake, scripts, __pycache__, dan orders), 8 file utama (meliputi 4 file .py, 1 Dockerfile, 1 file .yml, 1 .txt, dan 1 .gitignore), serta file dinamis pada folder dags/__pycache__/ yang berisikan cache agar kode python bisa lebih efisien dan data_lake/orders/ yang berisikan raw data dalam bentuk parquet.
