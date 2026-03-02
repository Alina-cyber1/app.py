# create_data.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

print("🔄 Создаю файлы с данными...")
os.makedirs('data/processed', exist_ok=True)

def create_realistic_data(domain, num_papers, start_year=2010, end_year=2025):
    np.random.seed(42 if domain == 'semiconductors' else 123)
    data = []
    semiconductor_topics = ['CMOS technology', 'FinFET devices', 'EUV lithography', 'GaN semiconductors', 'SiC power devices', 'Quantum dots', '2D materials', 'Memristors', 'Spintronics', 'Advanced packaging']
    gene_topics = ['CRISPR-Cas9', 'Gene therapy', 'CAR-T cells', 'mRNA vaccines', 'Base editing', 'Prime editing', 'AAV vectors', 'Lipid nanoparticles', 'Stem cells', 'Epigenetics']
    institutes = ['MIT', 'Stanford University', 'Harvard University', 'University of Cambridge', 'Tsinghua University', 'Max Planck Institute', 'ETH Zurich', 'University of Tokyo', 'Caltech', 'University of Oxford']
    topics = semiconductor_topics if domain == 'semiconductors' else gene_topics

    for i in range(num_papers):
        year = np.random.randint(start_year, end_year + 1)
        month = np.random.randint(1, 13)
        day = np.random.randint(1, 28)
        date = datetime(year, month, day)
        topic = np.random.choice(topics)
        verbs = ['advances in', 'novel approach to', 'review of', 'breakthrough in', 'study of']
        adj = ['high-performance', 'efficient', 'scalable', 'robust', 'integrated']
        title_parts = [f"{np.random.choice(verbs).capitalize()} {topic}", f"{np.random.choice(adj)} {topic} for {np.random.choice(['applications', 'devices', 'systems'])}", f"{topic}: {np.random.choice(['a comprehensive review', 'recent progress', 'future perspectives'])}"]
        title = np.random.choice(title_parts)
        num_authors = np.random.randint(1, 6)
        authors = []
        for j in range(num_authors):
            first = chr(65 + np.random.randint(0, 26))
            last = chr(65 + np.random.randint(0, 26))
            authors.append(f"{first}. {last}.")
        authors_str = ', '.join(authors)
        institute = np.random.choice(institutes)
        age_factor = 1 - (year - start_year) / (end_year - start_year)
        citations = int(np.random.poisson(20 * age_factor) + np.random.randint(0, 10))
        doi = f"10.1016/j.{domain}.{year}.{np.random.randint(1000, 9999)}"
        openalex_id = f"https://openalex.org/W{np.random.randint(1000000, 9999999)}"
        data.append({
            'id': openalex_id,
            'title': title,
            'publication_date': date.strftime('%Y-%m-%d'),
            'year': year,
            'cited_by_count': citations,
            'doi': doi,
            'type': 'article',
            'domain': domain,
            'authors': authors_str,
            'institution': institute,
            'topic': topic,
            'abstract': f"This paper presents {np.random.choice(verbs)} in {topic}..."
        })
    return pd.DataFrame(data)

# Полупроводники
print("📊 Создаю данные для полупроводников...")
df_semi = create_realistic_data('semiconductors', 5000).sort_values('publication_date')
df_semi.to_parquet('data/processed/semiconductors_clean.parquet', index=False)
print(f"✅ Сохранено {len(df_semi)} статей")

# Генная инженерия
print("🧬 Создаю данные для генной инженерии...")
df_gene = create_realistic_data('gene_engineering', 3500).sort_values('publication_date')
df_gene.to_parquet('data/processed/gene_engineering_clean.parquet', index=False)
print(f"✅ Сохранено {len(df_gene)} статей")

=======
# create_data.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

print("🔄 Создаю файлы с данными...")
os.makedirs('data/processed', exist_ok=True)

def create_realistic_data(domain, num_papers, start_year=2010, end_year=2025):
    np.random.seed(42 if domain == 'semiconductors' else 123)
    data = []
    semiconductor_topics = ['CMOS technology', 'FinFET devices', 'EUV lithography', 'GaN semiconductors', 'SiC power devices', 'Quantum dots', '2D materials', 'Memristors', 'Spintronics', 'Advanced packaging']
    gene_topics = ['CRISPR-Cas9', 'Gene therapy', 'CAR-T cells', 'mRNA vaccines', 'Base editing', 'Prime editing', 'AAV vectors', 'Lipid nanoparticles', 'Stem cells', 'Epigenetics']
    institutes = ['MIT', 'Stanford University', 'Harvard University', 'University of Cambridge', 'Tsinghua University', 'Max Planck Institute', 'ETH Zurich', 'University of Tokyo', 'Caltech', 'University of Oxford']
    topics = semiconductor_topics if domain == 'semiconductors' else gene_topics

    for i in range(num_papers):
        year = np.random.randint(start_year, end_year + 1)
        month = np.random.randint(1, 13)
        day = np.random.randint(1, 28)
        date = datetime(year, month, day)
        topic = np.random.choice(topics)
        verbs = ['advances in', 'novel approach to', 'review of', 'breakthrough in', 'study of']
        adj = ['high-performance', 'efficient', 'scalable', 'robust', 'integrated']
        title_parts = [f"{np.random.choice(verbs).capitalize()} {topic}", f"{np.random.choice(adj)} {topic} for {np.random.choice(['applications', 'devices', 'systems'])}", f"{topic}: {np.random.choice(['a comprehensive review', 'recent progress', 'future perspectives'])}"]
        title = np.random.choice(title_parts)
        num_authors = np.random.randint(1, 6)
        authors = []
        for j in range(num_authors):
            first = chr(65 + np.random.randint(0, 26))
            last = chr(65 + np.random.randint(0, 26))
            authors.append(f"{first}. {last}.")
        authors_str = ', '.join(authors)
        institute = np.random.choice(institutes)
        age_factor = 1 - (year - start_year) / (end_year - start_year)
        citations = int(np.random.poisson(20 * age_factor) + np.random.randint(0, 10))
        doi = f"10.1016/j.{domain}.{year}.{np.random.randint(1000, 9999)}"
        openalex_id = f"https://openalex.org/W{np.random.randint(1000000, 9999999)}"
        data.append({
            'id': openalex_id,
            'title': title,
            'publication_date': date.strftime('%Y-%m-%d'),
            'year': year,
            'cited_by_count': citations,
            'doi': doi,
            'type': 'article',
            'domain': domain,
            'authors': authors_str,
            'institution': institute,
            'topic': topic,
            'abstract': f"This paper presents {np.random.choice(verbs)} in {topic}..."
        })
    return pd.DataFrame(data)

# Полупроводники
print("📊 Создаю данные для полупроводников...")
df_semi = create_realistic_data('semiconductors', 5000).sort_values('publication_date')
df_semi.to_parquet('data/processed/semiconductors_clean.parquet', index=False)
print(f"✅ Сохранено {len(df_semi)} статей")

# Генная инженерия
print("🧬 Создаю данные для генной инженерии...")
df_gene = create_realistic_data('gene_engineering', 3500).sort_values('publication_date')
df_gene.to_parquet('data/processed/gene_engineering_clean.parquet', index=False)
print(f"✅ Сохранено {len(df_gene)} статей")

print("\n🎉 Готово! Файлы созданы в папке data/processed/")
