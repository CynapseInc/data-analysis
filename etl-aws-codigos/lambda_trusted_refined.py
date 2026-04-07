import os
import sys
import subprocess
import boto3
import io

s3_client = boto3.client('s3')
BUCKET_NAME_IN = "trusted-encanto"
BUCKET_NAME_OUT = "refined-encanto"

def install_dependencies():
    """Instala o matplotlib e TODAS as dependências mínimas no /tmp"""
    if not os.path.exists("/tmp/matplotlib") or not os.path.exists("/tmp/pyparsing"):
        print("Limpando e instalando Matplotlib + Dependências Completas no /tmp...")
        subprocess.run(["rm", "-rf", "/tmp/*"]) 
        
        dependencies = [
            "matplotlib", 
            "Pillow", 
            "cycler", 
            "kiwisolver", 
            "fonttools", 
            "packaging", 
            "pyparsing", 
            "python-dateutil",
            "contourpy"
        ]
        
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", 
            *dependencies,
            "--no-deps", "-t", "/tmp", "--quiet"
        ])
    
    if "/tmp" not in sys.path:
        sys.path.append("/tmp")

def save_to_s3(plt_obj, file_name):
    """Envia o gráfico atual para o S3 e limpa a memória"""
    img_data = io.BytesIO()
    plt_obj.savefig(img_data, format='png', bbox_inches='tight')
    img_data.seek(0)
    s3_client.put_object(
        Body=img_data,
        Bucket=BUCKET_NAME_OUT,
        Key=f'refined/{file_name}.png',
        ContentType='image/png'
    )
    plt_obj.close()

def lambda_handler(event, context):
    install_dependencies()
    
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import pandas as pd
    import numpy as np

    try:
        key = event['Records'][0]['s3']['object']['key']
    except (KeyError, IndexError, TypeError):
        key = 'tv_shows_trusted.csv'
        
    print(f"Lendo arquivo: {key}")
    obj = s3_client.get_object(Bucket=BUCKET_NAME_IN, Key=key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        
    # 1. Faixa Etária
    plt.figure()
    df['Age'].value_counts().plot(kind='bar', color='skyblue')
    plt.title('Quantidade por faixa etária')
    save_to_s3(plt, 'qtdFaixaEtaria')

    # 2. Por Plataforma
    platforms = ['Netflix', 'Hulu', 'Prime Video', 'Disney+']
    counts = [df[df[p] == 'SIM'].shape[0] for p in platforms]
    plt.figure()
    plt.bar(platforms, counts, color=['#FFB6C1', '#ADD8E6', '#98FB98', '#FFDAB9'])
    plt.title('Quantidade por plataforma')
    save_to_s3(plt, 'qtdPorPlataforma')
 
    # 3. Distribuição IMDb
    plt.figure()
    df['imdb_100'].plot(kind='hist', bins=20, color='#FFB6C1')
    plt.title('Distribuição das notas IMDb')
    save_to_s3(plt, 'distribuicaoIMDb')

    # 4. Distribuição Rotten Tomatoes
    plt.figure()
    df['rottenTomatoes_100'].plot(kind='hist', bins=20, color='#ADD8E6')
    plt.title('Distribuição das notas Rotten Tomatoes')
    save_to_s3(plt, 'distribuicaoRotten')

    # Scores e Filtros
    df['score'] = df[['imdb_100', 'rottenTomatoes_100']].mean(axis=1)
    adulto = df[df['Age'].isin(['16+', '18+'])].copy()
    infantil = df[df['Age'].isin(['all', '7+', '13+'])].copy()

    # 5. Top 10 Adulto
    plt.figure()
    top10_a = adulto.sort_values('score', ascending=False).head(10)
    plt.barh(top10_a['Title'], top10_a['score'], color='#ADD8E6')
    plt.gca().invert_yaxis()
    plt.title('Top 10 séries - Público adulto')
    save_to_s3(plt, 'top10_adulto')

    # 6. Top 10 Infantil
    plt.figure()
    top10_i = infantil.sort_values('score', ascending=False).head(10)
    plt.barh(top10_i['Title'], top10_i['score'], color='#FFB6C1')
    plt.gca().invert_yaxis()
    plt.title('Top 10 séries - Público Infantil')
    save_to_s3(plt, 'top10_infantil')

    # 7. Piores Adulto
    plt.figure()
    worst_a = adulto.sort_values('score').head(10)
    plt.barh(worst_a['Title'], worst_a['score'], color='#ADD8E6')
    plt.gca().invert_yaxis()
    plt.title('Piores séries - Público adulto')
    save_to_s3(plt, 'piores_adulto')

    # 8. Piores Infantil
    plt.figure()
    worst_i = infantil.sort_values('score').head(10)
    plt.barh(worst_i['Title'], worst_i['score'], color='#FFB6C1')
    plt.gca().invert_yaxis()
    plt.title('Piores séries - Público infantil')
    save_to_s3(plt, 'piores_infantil')

    # 9. Disponibilidade Multiplataforma
    for p in platforms:
        df[p + '_bin'] = (df[p].astype(str).str.strip().str.upper() == 'SIM').astype(int)
    df['Total_Plataformas'] = df[[p + '_bin' for p in platforms]].sum(axis=1)
    top_disp = df.sort_values(by='Total_Plataformas', ascending=False).head(10)
    plt.figure(figsize=(12, 6))
    plt.barh(top_disp['Title'], top_disp['Total_Plataformas'], color='#FFB6C1')
    plt.title('Top Disponibilidade Multiplataforma')
    plt.gca().invert_yaxis()
    save_to_s3(plt, 'disponibilidadeMultiplataforma')

    # 10. Melhores vs Piores
    top10_g = df.sort_values('score', ascending=False).head(10).assign(tipo='Melhores')
    worst10_g = df.sort_values('score').head(10).assign(tipo='Piores')
    combined = pd.concat([top10_g, worst10_g]).sort_values('score')
    plt.figure()
    for tipo, grupo in combined.groupby('tipo'):
        plt.barh(grupo['Title'], grupo['score'], label=tipo, color='#ADD8E6' if tipo == 'Melhores' else '#FFB6C1')
    plt.legend()
    plt.title('Melhores vs Piores Séries')
    save_to_s3(plt, 'melhores_piores_series')

    return {"status": "Processo concluído: 10 gráficos gerados."}