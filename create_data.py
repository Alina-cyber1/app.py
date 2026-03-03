import pandas as pd
import numpy as np
from datetime import datetime
import os
import pyarrow as pa
import pyarrow.parquet as pq

print("🔄 Создаю файлы с данными...")
os.makedirs('data/processed', exist_ok=True)

def create_realistic_data(domain, num_papers, start_year=2010, end_year=2025):
    """Создает реалистичные данные"""
    np.random.seed(42 if domain == 'semiconductors' else 123)
    data = []
    
    semiconductor_topics = ['CMOS technology', 'FinFET devices', 'EUV lithography', 'GaN semiconductors', 
                           'SiC power devices', 'Quantum dots', '2D materials', 'Memristors', 
                           'Spintronics', 'Advanced packaging']
    
    gene_topics = ['CRISPR-Cas9', 'Gene therapy', 'CAR-T cells', 'mRNA vaccines', 
                  'Base editing', 'Prime editing', 'AAV vectors', 'Lipid nanoparticles', 
                  'Stem cells', 'Epigenetics']
    
    institutes = ['MIT', 'Stanford', 'Harvard', 'Cambridge', 'Tsinghua', 
                  'Max Planck', 'ETH Zurich', 'Tokyo', 'Caltech', 'Oxford']
    
    topics = semiconductor_topics if domain == 'semiconductors' else gene_topics
    
    for i in range(num_papers):
        year = np.random.randint(start_year, end_year + 1)
        month = np.random.randint(1, 13)
        day = np.random.randint(1, 28)
        date = datetime(year, month, day)
        topic = np.random.choice(topics)
        
        # Простой заголовок
        title = f"Paper on {topic} - {np.random.randint(1000, 9999)}"
        
        # Авторы
        num_authors = np.random.randint(1, 4)
        authors = [f"Author {chr(65 + np.random.randint(0, 26))}" for _ in range(num_authors)]
        authors_str = ', '.join(authors)
        
        institute = np.random.choice(institutes)
        
        # Цитирования
        citations = np.random.poisson(10) + np.random.randint(0, 20)
        
        data.append({
            'publication_date': date.strftime('%Y-%m-%d'),
            'title': title,
            'cited_by_count': citations,
            'authors': authors_str,
            'institution': institute,
            'topic': topic
        })
    
    df = pd.DataFrame(data)
    return df.sort_values('publication_date')

# Создаем данные
print("📊 Создаю данные для полупроводников...")
df_semi = create_realistic_data('semiconductors', 1000)  # Уменьшил для скорости
df_semi.to_parquet('data/processed/semiconductors_clean.parquet', engine='pyarrow', index=False)
print(f"✅ Сохранено {len(df_semi)} статей")

print("🧬 Создаю данные для генной инженерии...")
df_gene = create_realistic_data('gene_engineering', 800)  # Уменьшил для скорости
df_gene.to_parquet('data/processed/gene_engineering_clean.parquet', engine='pyarrow', index=False)
print(f"✅ Сохранено {len(df_gene)} статей")

print("\n🎉 Готово!")
