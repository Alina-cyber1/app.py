import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import numpy as np
import io
import matplotlib.pyplot as plt

# ========== ИМПОРТЫ ДЛЯ PDF ==========
try:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import letter, A4
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import inch
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False
    st.warning("⚠️ Для экспорта PDF установите: pip install reportlab matplotlib")

# ========== НАСТРОЙКА СТРАНИЦЫ ==========
st.set_page_config(
    page_title="Tech Trends Monitor",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ========== КРАСИВЫЙ CSS ==========
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 3rem;
        font-weight: 700;
        margin-bottom: 0;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 10px;
        padding: 20px;
        color: white;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .domain-card {
        background: white;
        border-radius: 10px;
        padding: 20px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        border-left: 5px solid #667eea;
        margin-bottom: 10px;
    }
    .stButton > button {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 5px;
        padding: 10px 20px;
        font-weight: 600;
        transition: transform 0.3s ease;
    }
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(102,126,234,0.4);
    }
    .stDownloadButton > button {
        background: linear-gradient(90deg, #00CC96 0%, #00B386 100%);
    }
</style>
""", unsafe_allow_html=True)

# ========== ФУНКЦИЯ ГЕНЕРАЦИИ PDF ==========
def generate_pdf_report(domain_name, metrics, dates, papers, patents):
    """Генерирует PDF отчет с графиками и метриками"""
    
    if not PDF_AVAILABLE:
        return None
    
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4)
    styles = getSampleStyleSheet()
    story = []
    
    # Заголовок
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=colors.HexColor('#667eea'),
        alignment=1,
        spaceAfter=30
    )
    
    title = Paragraph(f"🚀 Tech Trends Report: {domain_name}", title_style)
    story.append(title)
    
    # Дата
    date_style = ParagraphStyle(
        'DateStyle',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.gray,
        alignment=1
    )
    date_text = Paragraph(f"Сгенерировано: {datetime.now().strftime('%d.%m.%Y %H:%M')}", date_style)
    story.append(date_text)
    story.append(Spacer(1, 20))
    
    # Ключевые метрики
    story.append(Paragraph("📊 Ключевые метрики", styles['Heading2']))
    story.append(Spacer(1, 10))
    
    metrics_data = [
        ['Показатель', 'Значение', 'Изменение'],
        ['Научные публикации', str(metrics['papers_total']), f"+{metrics['papers_growth']}%"],
        ['Патенты', str(metrics['patents_total']), f"+{metrics['patents_growth']}%"],
        ['Time Lag', f"{metrics['time_lag']} года", str(metrics['time_lag_change'])],
        ['Trend Score', f"{metrics['trend_score']}/100", metrics['trend_status']],
        ['AI-интеграция', f"{metrics['ai_share']}%", "в патентах"]
    ]
    
    metrics_table = Table(metrics_data, colWidths=[150, 100, 100])
    metrics_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#667eea')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f8f9fa')),
        ('GRID', (0, 0), (-1, -1), 1, colors.lightgrey),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
    ]))
    story.append(metrics_table)
    story.append(Spacer(1, 20))
    
    # Топ заявителей
    story.append(Paragraph("🏭 Топ-5 заявителей", styles['Heading2']))
    story.append(Spacer(1, 10))
    
    assignees_data = [['Компания', 'Количество патентов']]
    for i in range(len(metrics['top_assignees'])):
        assignees_data.append([metrics['top_assignees'][i], str(metrics['assignee_values'][i])])
    
    assignees_table = Table(assignees_data, colWidths=[200, 100])
    assignees_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#00CC96')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f8f9fa')),
        ('GRID', (0, 0), (-1, -1), 1, colors.lightgrey),
    ]))
    story.append(assignees_table)
    story.append(Spacer(1, 20))
    
    # География
    story.append(Paragraph("🌍 География патентования", styles['Heading2']))
    story.append(Spacer(1, 10))
    
    geo_data = [['Страна', 'Доля (%)']]
    for i in range(len(metrics['countries'])):
        geo_data.append([metrics['countries'][i], str(metrics['country_values'][i])])
    
    geo_table = Table(geo_data, colWidths=[150, 100])
    geo_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#FF4B4B')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f8f9fa')),
        ('GRID', (0, 0), (-1, -1), 1, colors.lightgrey),
    ]))
    story.append(geo_table)
    story.append(Spacer(1, 20))
    
    # График динамики
    story.append(Paragraph("📈 Динамика развития (последние 2 года)", styles['Heading2']))
    story.append(Spacer(1, 10))
    
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(dates[-24:], papers[-24:], label='Публикации', color='#00CC96', linewidth=2)
    ax.plot(dates[-24:], patents[-24:], label='Патенты', color='#FF4B4B', linewidth=2)
    ax.set_xlabel('Год')
    ax.set_ylabel('Количество')
    ax.set_title(f'{domain_name}: Динамика за 2 года')
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.set_facecolor('#f8f9fa')
    fig.patch.set_facecolor('#f8f9fa')
    
    img_buffer = io.BytesIO()
    fig.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight', facecolor='#f8f9fa')
    plt.close(fig)
    img_buffer.seek(0)
    
    story.append(Image(img_buffer, width=450, height=250))
    story.append(Spacer(1, 20))
    
    # Подтехнологии
    story.append(Paragraph("🔬 Быстрорастущие подтехнологии", styles['Heading2']))
    story.append(Spacer(1, 10))
    
    if "Полупроводники" in domain_name:
        subtopics = ["Квантовые вычисления", "Advanced Packaging", "GaN/SiC устройства", "EUV литография", "MRAM память"]
        growth = [55, 45, 38, 42, 28]
    else:
        subtopics = ["CRISPR-Cas12/13", "Липидные наночастицы", "CAR-T терапия", "Base editing", "Вирусные векторы"]
        growth = [68, 73, 52, 48, 41]
    
    subtopics_data = [['Подтехнология', 'Рост за год (%)']]
    for i in range(len(subtopics)):
        subtopics_data.append([subtopics[i], str(growth[i])])
    
    subtopics_table = Table(subtopics_data, colWidths=[250, 100])
    subtopics_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#764ba2')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f8f9fa')),
        ('GRID', (0, 0), (-1, -1), 1, colors.lightgrey),
    ]))
    story.append(subtopics_table)
    story.append(Spacer(1, 30))
    
    # Рекомендации
    story.append(Paragraph("💡 Рекомендации", styles['Heading2']))
    story.append(Spacer(1, 10))
    
    if metrics['trend_score'] > 80:
        recs = [
            "🔥 Технология показывает взрывной рост. Рекомендуется:",
            "• Активно инвестировать в R&D",
            "• Усилить патентную защиту",
            "• Мониторить стартапы в этой области"
        ]
    elif metrics['trend_score'] > 60:
        recs = [
            "📈 Стабильный рост. Рекомендуется:",
            "• Продолжать текущие разработки",
            "• Анализировать конкурентные патенты",
            "• Рассмотреть стратегические партнерства"
        ]
    else:
        recs = [
            "💤 Технология в стадии созревания. Рекомендуется:",
            "• Оптимизировать существующие решения",
            "• Искать новые ниши применения",
            "• Мониторить смежные области"
        ]
    
    for rec in recs:
        story.append(Paragraph(rec, styles['Normal']))
        story.append(Spacer(1, 3))
    
    doc.build(story)
    buffer.seek(0)
    return buffer

# ========== ЗАГРУЗКА ДАННЫХ ==========
@st.cache_data(ttl=3600)
def load_domain_data(domain_name):
    """Загружает данные в зависимости от выбранного домена"""
    
    if "Полупроводники" in domain_name:
        dates = pd.date_range(start='2018-01-01', end='2025-01-01', freq='M')
        np.random.seed(42)
        papers = 40 + np.cumsum(np.random.randn(len(dates)) * 1.5 + 1.5)
        patents = 20 + np.cumsum(np.random.randn(len(dates)) * 1.2 + 1.0)
        metrics = {
            'papers_total': 1234,
            'papers_growth': 12,
            'patents_total': 892,
            'patents_growth': 8,
            'time_lag': 3.2,
            'time_lag_change': -0.5,
            'trend_score': 78,
            'trend_status': '📈 Emerging',
            'top_assignees': ['TSMC', 'Intel', 'Samsung', 'Qualcomm', 'Micron'],
            'assignee_values': [234, 189, 156, 98, 76],
            'countries': ['US', 'CN', 'JP', 'KR', 'EP'],
            'country_values': [45, 25, 12, 10, 8],
            'ai_share': 32
        }
    else:
        dates = pd.date_range(start='2018-01-01', end='2025-01-01', freq='M')
        np.random.seed(123)
        papers = 30 + np.cumsum(np.random.randn(len(dates)) * 1.8 + 2.0)
        patents = 15 + np.cumsum(np.random.randn(len(dates)) * 1.5 + 1.2)
        metrics = {
            'papers_total': 2156,
            'papers_growth': 28,
            'patents_total': 743,
            'patents_growth': 35,
            'time_lag': 4.8,
            'time_lag_change': -1.2,
            'trend_score': 92,
            'trend_status': '🔥 Hot',
            'top_assignees': ['Editas Medicine', 'CRISPR Therapeutics', 'Intellia', 'Vertex', 'Moderna'],
            'assignee_values': [145, 132, 98, 67, 54],
            'countries': ['US', 'CN', 'EP', 'JP', 'KR'],
            'country_values': [58, 18, 12, 7, 5],
            'ai_share': 18
        }
    return dates, papers, patents, metrics

# ========== ЗАГОЛОВОК ==========
st.markdown('<h1 class="main-header">🚀 Tech Trends Monitor</h1>', unsafe_allow_html=True)
st.markdown("*Отслеживание перехода науки в технологии*")

# ========== САЙДБАР ==========
with st.sidebar:
    st.markdown("## 🎛 Панель управления")
    st.markdown("---")
    
    domain = st.selectbox(
        "🔬 Технологический домен",
        ["💻 Полупроводники", "🧬 Генная инженерия"],
        help="Выберите технологическую область для анализа"
    )
    
    domain_clean = domain.replace("💻 ", "").replace("🧬 ", "")
    
    if "Полупроводники" in domain:
        st.markdown("""
        <div class="domain-card">
            <b>📋 Коды CPC:</b><br>
            H01L, H10, G03F, C23C, H05K<br>
            <b>🎯 Topics:</b><br>
            Semiconductor devices, Lithography, Thin films
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div class="domain-card">
            <b>📋 Коды CPC:</b><br>
            C12N15/00, A61K48/00, C12N9/22<br>
            <b>🎯 Topics:</b><br>
            CRISPR/Cas, Gene therapy, Vectors
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    st.markdown("### 📅 Период анализа")
    year_range = st.slider("Выберите годы", 2015, 2025, (2020, 2025), label_visibility="collapsed")
    
    st.markdown("### 🌍 Страны патентования")
    col1, col2 = st.columns(2)
    with col1:
        us = st.checkbox("🇺🇸 US", value=True)
        cn = st.checkbox("🇨🇳 CN", value=True)
        jp = st.checkbox("🇯🇵 JP", value=False)
    with col2:
        kr = st.checkbox("🇰🇷 KR", value=False)
        ep = st.checkbox("🇪🇺 EP", value=True)
        wo = st.checkbox("🌐 WO", value=False)
    
    st.markdown("---")
    st.markdown("### 📊 Статус системы")
    dates, papers, patents, metrics = load_domain_data(domain_clean)
    
    status_col1, status_col2 = st.columns(2)
    with status_col1:
        st.markdown("🟢 OpenAlex")
        st.markdown("🟡 BigQuery")
    with status_col2:
        st.markdown(f"✅ {metrics['papers_total']} статей")
        st.markdown(f"⏳ {metrics['patents_total']} патентов")
    st.progress(0.8, text="Готовность MVP")

# ========== ЗАГРУЗКА ДАННЫХ ==========
dates, papers, patents, metrics = load_domain_data(domain_clean)

# ========== МЕТРИКИ ==========
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.markdown(f"""
    <div class="metric-card">
        <h3 style="color: white; margin:0">📄 Публикации</h3>
        <h1 style="color: white; margin:0">{metrics['papers_total']}</h1>
        <p style="color: white; margin:0">+{metrics['papers_growth']}% за год</p>
    </div>
    """, unsafe_allow_html=True)
with col2:
    st.markdown(f"""
    <div class="metric-card">
        <h3 style="color: white; margin:0">📃 Патенты</h3>
        <h1 style="color: white; margin:0">{metrics['patents_total']}</h1>
        <p style="color: white; margin:0">+{metrics['patents_growth']}% за год</p>
    </div>
    """, unsafe_allow_html=True)
with col3:
    st.markdown(f"""
    <div class="metric-card">
        <h3 style="color: white; margin:0">⏱ Time Lag</h3>
        <h1 style="color: white; margin:0">{metrics['time_lag']} года</h1>
        <p style="color: white; margin:0">{metrics['time_lag_change']} vs 2024</p>
    </div>
    """, unsafe_allow_html=True)
with col4:
    st.markdown(f"""
    <div class="metric-card">
        <h3 style="color: white; margin:0">🎯 Trend Score</h3>
        <h1 style="color: white; margin:0">{metrics['trend_score']}/100</h1>
        <p style="color: white; margin:0">{metrics['trend_status']}</p>
    </div>
    """, unsafe_allow_html=True)

# ========== ОСНОВНОЙ КОНТЕНТ ==========
tab1, tab2, tab3, tab4 = st.tabs(["📈 Тренды", "🔬 Подтехнологии", "📊 Данные", "📄 Отчеты"])

with tab1:
    st.markdown(f"## 📊 Динамика развития: {domain_clean}")
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=dates, y=papers, 
        name='📄 Научные публикации', 
        line=dict(color='#00CC96', width=4),
        fill='tozeroy', 
        fillcolor='rgba(0,204,150,0.1)'
    ))
    fig.add_trace(go.Scatter(
        x=dates, y=patents, 
        name='📃 Патенты', 
        line=dict(color='#FF4B4B', width=4),
        fill='tozeroy', 
        fillcolor='rgba(255,75,75,0.1)',
        yaxis='y2'
    ))
    
    fig.update_layout(
        title=f"{domain_clean}: Наука vs Технологии",
        xaxis_title="Год",
        yaxis_title="Количество публикаций",
        yaxis2=dict(
            title="Количество патентов",
            overlaying='y',
            side='right'
        ),
        hovermode='x unified',
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        height=500,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    st.plotly_chart(fig, use_container_width=True)
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("### 🏭 Топ заявителей")
        top_df = pd.DataFrame({
            'Компания': metrics['top_assignees'], 
            'Патенты': metrics['assignee_values']
        })
        fig2 = px.bar(
            top_df, x='Компания', y='Патенты', 
            color='Патенты', 
            color_continuous_scale='Viridis'
        )
        fig2.update_layout(
            plot_bgcolor='rgba(0,0,0,0)', 
            paper_bgcolor='rgba(0,0,0,0)', 
            showlegend=False
        )
        st.plotly_chart(fig2, use_container_width=True)
    
    with col2:
        st.markdown("### 🌍 География")
        geo_df = pd.DataFrame({
            'Страна': metrics['countries'], 
            'Доля': metrics['country_values']
        })
        fig3 = px.pie(
            geo_df, values='Доля', names='Страна',
            color_discrete_sequence=px.colors.sequential.Viridis
        )
        fig3.update_traces(textposition='inside', textinfo='percent+label')
        fig3.update_layout(showlegend=False)
        st.plotly_chart(fig3, use_container_width=True)

with tab2:
    st.markdown(f"## 🔬 Подтехнологии в {domain_clean}")
    
    if "Полупроводники" in domain:
        subtopics = ["Литография (EUV/DUV)", "Advanced Packaging", "GaN/SiC устройства", "MRAM/FRAM память", "Квантовые вычисления"]
        growth = [45, 38, 32, 28, 55]
    else:
        subtopics = ["CRISPR-Cas9", "CRISPR-Cas12/13", "Вирусные векторы (AAV)", "Липидные наночастицы", "CAR-T терапия"]
        growth = [52, 68, 41, 73, 47]
    
    sub_df = pd.DataFrame({
        'Технология': subtopics, 
        'Рост за год (%)': growth
    })
    
    fig4 = px.bar(
        sub_df, x='Технология', y='Рост за год (%)',
        color='Рост за год (%)',
        color_continuous_scale='Viridis',
        title="Темпы роста подтехнологий"
    )
    fig4.update_layout(
        plot_bgcolor='rgba(0,0,0,0)', 
        paper_bgcolor='rgba(0,0,0,0)'
    )
    st.plotly_chart(fig4, use_container_width=True)
    
    st.markdown("---")
    st.markdown("### 🔍 Детальный анализ")
    
    for i, (sub, grow) in enumerate(zip(subtopics[:3], growth[:3])):
        with st.expander(f"📌 {sub} (Рост: +{grow}%)"):
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"**Публикаций:** {np.random.randint(100, 500)}")
                st.markdown(f"**Патентов:** {np.random.randint(50, 300)}")
                st.markdown(f"**Time Lag:** {np.random.uniform(2, 5):.1f} года")
            with col2:
                st.markdown(f"**Топ-заявитель:** {np.random.choice(metrics['top_assignees'])}")
                st.markdown(f"**AI-интеграция:** {np.random.randint(10, 60)}%")
                st.markdown(f"**Trend Score:** {np.random.randint(65, 95)}/100")

with tab3:
    st.markdown(f"## 📊 Данные по {domain_clean}")
    
    example_data = pd.DataFrame({
        'Дата': dates[:20],
        'Название': [f'{domain_clean} - публикация {i}' for i in range(20)],
        'Цитирования': np.random.randint(10, 100, 20),
        'Авторы': [f'Author {i}, Author {i+1}' for i in range(20)],
        'Тип': ['Научная статья'] * 20
    })
    
    st.dataframe(
        example_data,
        column_config={
            "Дата": st.column_config.DateColumn("Дата публикации"),
            "Цитирования": st.column_config.NumberColumn("Цитирований", format="%d ⭐"),
        },
        hide_index=True,
        use_container_width=True
    )
    
    col1, col2, col3 = st.columns([1, 1, 2])
    with col1:
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

with tab4:
    st.markdown("## 📄 Генерация отчетов")
    
    if not PDF_AVAILABLE:
        st.error("⚠️ Для экспорта PDF установите библиотеки:")
        st.code("pip install reportlab matplotlib")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div style='background: linear-gradient(135deg, #667eea10, #764ba210); padding: 20px; border-radius: 10px; border: 1px solid #667eea30;'>
            <h3 style='margin:0; color: #667eea;'>🎴 Topic Card</h3>
            <p style='color: gray;'>⚡ Быстрый отчет по технологии</p>
            <hr style='margin: 10px 0;'>
            <p>✅ Название темы + домен</p>
            <p>✅ Ключевые метрики</p>
            <p>✅ Топ-5 игроков</p>
            <p>✅ График динамики</p>
            <p>✅ Рекомендации</p>
        </div>
        """, unsafe_allow_html=True)
        
        if st.button("🔄 Сгенерировать Topic Card", key="topic_btn", use_container_width=True):
            with st.spinner("🔄 Генерация PDF отчета..."):
                pdf_buffer = generate_pdf_report(domain_clean, metrics, dates, papers, patents)
                if pdf_buffer:
                    st.success("✅ Topic Card готов!")
                    st.balloons()
                    st.download_button(
                        label="📥 Скачать PDF-отчет",
                        data=pdf_buffer,
                        file_name=f"topic_card_{domain_clean.lower().replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.pdf",
                        mime="application/pdf",
                        use_container_width=True
                    )
    
    with col2:
        st.markdown("""
        <div style='background: linear-gradient(135deg, #FF4B4B10, #FF6B6B10); padding: 20px; border-radius: 10px; border: 1px solid #FF4B4B30;'>
            <h3 style='margin:0; color: #FF4B4B;'>📊 Monthly Deep Dive</h3>
            <p style='color: gray;'>🔍 Детальный анализ с рекомендациями</p>
            <hr style='margin: 10px 0;'>
            <p>✅ Executive summary</p>
            <p>✅ Наука vs Патенты</p>
            <p>✅ Кластеры подтехнологий</p>
            <p>✅ Конкурентный анализ</p>
            <p>✅ Список "что читать"</p>
        </div>
        """, unsafe_allow_html=True)
        
        if st.button("🔬 Сгенерировать Deep Dive", key="deep_btn", use_container_width=True):
            with st.spinner("🔄 Генерация детального отчета..."):
                pdf_buffer = generate_pdf_report(domain_clean, metrics, dates, papers, patents)
                if pdf_buffer:
                    st.success("✅ Deep Dive отчет готов!")
                    st.snow()
                    st.download_button(
                        label="📥 Скачать PDF (Deep Dive)",
                        data=pdf_buffer,
                        file_name=f"deep_dive_{domain_clean.lower().replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.pdf",
                        mime="application/pdf",
                        use_container_width=True
                    )
    
    st.markdown("---")
    st.markdown("### 📋 Быстрый экспорт")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("📊 Экспорт метрик в PDF", use_container_width=True):
            with st.spinner("🔄 Генерация PDF..."):
                pdf_buffer = generate_pdf_report(domain_clean, metrics, dates, papers, patents)
                if pdf_buffer:
                    st.download_button(
                        label="📥 Скачать PDF",
                        data=pdf_buffer,
                        file_name=f"metrics_{domain_clean.lower().replace(' ', '_')}.pdf",
                        mime="application/pdf",
                        use_container_width=True
                    )
    with col2:
        timeline_data = pd.DataFrame({
            'Дата': dates,
            'Публикации': papers.astype(int),
            'Патенты': patents.astype(int)
        })
        csv = timeline_data.to_csv(index=False).encode('utf-8')
        st.download_button(
            "📥 Скачать CSV",
            csv,
            f"{domain_clean.lower().replace(' ', '_')}_timeline.csv",
            "text/csv",
            use_container_width=True
        )
    with col3:
        if st.button("📧 Отправить по email", disabled=True, use_container_width=True):
            st.info("Функция в разработке")

# ========== ФУТЕР ==========
st.markdown("---")
st.markdown(f"""
<div style='text-align: center; color: gray; padding: 20px;'>
    <p style='font-size: 1.2em;'>🚀 Data Science платформа мониторинга технологических трендов | MVP v1.0</p>
    <p style='font-size: 0.9em;'>
        Текущий домен: {domain_clean} | 
        Данные: OpenAlex, Google Patents | 
        Обновлено: {datetime.now().strftime('%d.%m.%Y %H:%M')}
    </p>
    <p style='font-size: 0.8em; color: #999;'>
        Разработано в рамках стажировки | 2026
    </p>
</div>
""", unsafe_allow_html=True)

import os
import threading
from pyngrok import ngrok

# Функция запуска Streamlit
def run_streamlit():
    os.system("streamlit run app.py --server.port 8501 &")

# Запускаем Streamlit в фоне
thread = threading.Thread(target=run_streamlit)
thread.start()

# Открываем туннель через ngrok
public_url = ngrok.connect(addr='8501', proto='http')
print("🚀 Ваше приложение доступно по ссылке:", public_url)