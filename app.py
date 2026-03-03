with tab3:
    st.markdown(f"## 📊 Данные по {domain_clean}")

    # Загружаем DataFrame для показа примеров
    domain_map = {'Полупроводники': 'semiconductors', 'Генная инженерия': 'gene_engineering'}
    domain_key = domain_map.get(domain_clean, domain_clean.lower())
    file_path = f"data/processed/{domain_key}_clean.parquet"
    
    try:
        # Пытаемся загрузить реальные данные
        if os.path.exists(file_path):
            df_sample = pd.read_parquet(file_path)
            st.success(f"✅ Загружено {len(df_sample)} записей из реальных данных")
            
            # Выбираем колонки для отображения
            display_cols = ['publication_date', 'title', 'authors', 'topic', 'citations', 'affiliation']
            available_cols = [col for col in display_cols if col in df_sample.columns]
            
            if not available_cols:
                available_cols = df_sample.columns[:5].tolist()
            
            df_display = df_sample[available_cols].head(20)
            
            # Переименовываем колонки для красивого отображения
            column_names = {
                'publication_date': 'Дата публикации',
                'title': 'Название',
                'authors': 'Авторы',
                'topic': 'Тема',
                'citations': 'Цитирования',
                'affiliation': 'Организация'
            }
            df_display = df_display.rename(columns={col: column_names.get(col, col) for col in df_display.columns})
            
            st.dataframe(
                df_display,
                column_config={
                    "Дата публикации": st.column_config.DateColumn("Дата публикации"),
                    "Цитирования": st.column_config.NumberColumn("Цитирований", format="%d ⭐"),
                },
                hide_index=True,
                use_container_width=True
            )
            
            # Показываем статистику
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Всего записей", len(df_sample))
            with col2:
                st.metric("Уникальных тем", df_sample['topic'].nunique() if 'topic' in df_sample.columns else 'N/A')
            with col3:
                if 'affiliation' in df_sample.columns:
                    st.metric("Организаций", df_sample['affiliation'].nunique())
                else:
                    st.metric("Авторов", df_sample['authors'].nunique() if 'authors' in df_sample.columns else 'N/A')
                    
        else:
            st.warning(f"Файл {file_path} не найден")
            # Показываем синтетические данные
            example_data = pd.DataFrame({
                'Дата': dates[:20],
                'Название': [f'{domain_clean} - публикация {i}' for i in range(20)],
                'Цитирования': np.random.randint(10, 100, 20),
                'Авторы': [f'Author {i}, Author {i+1}' for i in range(20)],
                'Тип': ['Научная статья'] * 20
            })
            st.dataframe(example_data, hide_index=True, use_container_width=True)
            
    except Exception as e:
        st.error(f"❌ Ошибка загрузки данных: {e}")
        # Показываем синтетические данные
        example_data = pd.DataFrame({
            'Дата': dates[:20],
            'Название': [f'{domain_clean} - публикация {i}' for i in range(20)],
            'Цитирования': np.random.randint(10, 100, 20),
            'Авторы': [f'Author {i}, Author {i+1}' for i in range(20)],
            'Тип': ['Научная статья'] * 20
        })
        st.dataframe(example_data, hide_index=True, use_container_width=True)

    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
        # Кнопка скачивания CSV
        if 'df_sample' in locals() and len(df_sample) > 0:
            csv = df_sample.head(100).to_csv(index=False).encode('utf-8')
            st.download_button(
                "📥 Скачать CSV (первые 100 записей)",
                csv,
                f"{domain_clean.lower().replace(' ', '_')}_data.csv",
                "text/csv"
            )
        else:
            csv = example_data.to_csv(index=False).encode('utf-8')
            st.download_button(
                "📥 Скачать CSV",
                csv,
                f"{domain_clean.lower().replace(' ', '_')}_data.csv",
                "text/csv"
            )
    with col2:
        if st.button("📊 Экспорт в Excel", disabled=True):
            st.info("Функция в разработке")
    with col3:
        st.info(f"📈 AI-интеграция в домене: {metrics['ai_share']}% патентов содержат G06N*")
