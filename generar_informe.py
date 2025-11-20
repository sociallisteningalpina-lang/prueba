import pandas as pd
from pysentimiento import create_analyzer
import os
import json
import sys
from pathlib import Path

# Importar el clasificador de temas desde config
sys.path.insert(0, str(Path(__file__).parent / "config"))
from topic_classifier import create_topic_classifier, get_campaign_metadata


def run_report_generation():
    """
    Lee los datos del Excel, realiza el análisis de sentimientos y temas,
    y genera el panel HTML interactivo como 'index.html'.
    """
    print("--- INICIANDO GENERACIÓN DE INFORME HTML ---")
    
    try:
        df = pd.read_excel('Comentarios Campaña.xlsx')
        print("Archivo 'Comentarios Campaña.xlsx' cargado con éxito.")
    except FileNotFoundError:
        print("❌ ERROR: No se encontró el archivo 'Comentarios Campaña.xlsx'.")
        return

    # --- Limpieza y preparación de datos ---
    df['created_time_processed'] = pd.to_datetime(df['created_time_processed'])
    df['created_time_colombia'] = df['created_time_processed'] - pd.Timedelta(hours=5)

    # Asegurar que exista post_url_original (para archivos antiguos)
    if 'post_url_original' not in df.columns:
        print("⚠️  Nota: Creando post_url_original desde post_url")
        df['post_url_original'] = df['post_url'].copy()

    # --- Lógica de listado de pautas ---
    all_unique_posts = df[['post_url', 'post_url_original', 'platform']].drop_duplicates(subset=['post_url']).copy()
    all_unique_posts.dropna(subset=['post_url'], inplace=True)

    df_comments = df.dropna(subset=['created_time_colombia', 'comment_text', 'post_url']).copy()
    df_comments.reset_index(drop=True, inplace=True)

    comment_counts = df_comments.groupby('post_url').size().reset_index(name='comment_count')

    unique_posts = pd.merge(all_unique_posts, comment_counts, on='post_url', how='left')
    
    unique_posts.loc[:, 'comment_count'] = unique_posts['comment_count'].fillna(0).astype(int)
    
    unique_posts.sort_values(by='comment_count', ascending=False, inplace=True)
    unique_posts.reset_index(drop=True, inplace=True)
    
    post_labels = {}
    for index, row in unique_posts.iterrows():
        post_labels[row['post_url']] = f"Pauta {index + 1} ({row['platform']})"
    
    unique_posts['post_label'] = unique_posts['post_url'].map(post_labels)
    df_comments['post_label'] = df_comments['post_url'].map(post_labels)
    
    all_posts_json = json.dumps(unique_posts.to_dict('records'))

    print("Analizando sentimientos y temas...")
    
    # Análisis de sentimientos
    sentiment_analyzer = create_analyzer(task="sentiment", lang="es")
    df_comments['sentimiento'] = df_comments['comment_text'].apply(
        lambda text: {
            "POS": "Positivo", 
            "NEG": "Negativo", 
            "NEU": "Neutro"
        }.get(sentiment_analyzer.predict(str(text)).output, "Neutro")
    )
    
    # ========================================================================
    # CLASIFICACIÓN DE TEMAS - AHORA USANDO ARCHIVO EXTERNO
    # ========================================================================
    
    # Cargar el clasificador personalizado
    topic_classifier = create_topic_classifier()
    
    # Aplicar clasificación
    df_comments['tema'] = df_comments['comment_text'].apply(topic_classifier)
    
    # Mostrar metadata de la campaña (opcional)
    campaign_info = get_campaign_metadata()
    print(f"Usando clasificador: {campaign_info['campaign_name']} v{campaign_info['version']}")
    print(f"Categorías disponibles: {len(campaign_info['categories'])}")
    
    print("Análisis completado.")

    # Creamos el JSON para el dashboard
    df_for_json = df_comments[[
        'created_time_colombia', 'comment_text', 'sentimiento', 
        'tema', 'platform', 'post_url', 'post_label'
    ]].copy()
    
    df_for_json.rename(columns={
        'created_time_colombia': 'date', 
        'comment_text': 'comment', 
        'sentimiento': 'sentiment', 
        'tema': 'topic'
    }, inplace=True)
    
    df_for_json['date'] = df_for_json['date'].dt.strftime('%Y-%m-%dT%H:%M:%S')
    all_data_json = json.dumps(df_for_json.to_dict('records'))

    # Fechas min/max
    min_date = df_comments['created_time_colombia'].min().strftime('%Y-%m-%d') if not df_comments.empty else ''
    max_date = df_comments['created_time_colombia'].max().strftime('%Y-%m-%d') if not df_comments.empty else ''
    
    post_filter_options = '<option value="Todas">Ver Todas las Pautas</option>'
    for url, label in post_labels.items():
        post_filter_options += f'<option value="{url}">{label}</option>'

    # ... resto del código HTML sin cambios ...
    
    html_content = f"""
    <!DOCTYPE html>
    <html lang="es">
    <!-- ... tu HTML completo aquí ... -->
    </html>
    """
    
    report_filename = 'index.html'
    with open(report_filename, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"✅ Panel interactivo mejorado generado con éxito. Se guardó como '{report_filename}'.")


if __name__ == "__main__":
    run_report_generation()
