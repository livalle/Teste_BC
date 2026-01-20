import pandas as pd
import micropip
await micropip.install("openpyxl")

#importando tabela de dados populacionais ibge
df = pd.read_excel("Tabela 6579.xlsx")
df.head()

#renomeacao das colunas
colunas = ["municipio", "2015", "2016", "2017", "2018", "2019", "2020", "2021"]
df.columns = colunas

#limpeza do rodape da fonte
df = df[df["municipio"].notna()]
df = df[~df["municipio"].str.contains("Fonte", na=False)]
df_long = df.melt(id_vars="municipio", var_name="ano", value_name="populacao")

#limpeza dos dados
df_long["uf"] = df_long["municipio"].str.extract(r"\(([^)]+)\)$")
df_long["municipio_nome"] = df_long["municipio"].str.replace(r"\s*\([^)]+\)$", "", regex=True)
df_long["populacao"] = pd.to_numeric(df_long["populacao"], errors='coerce')
df_long.loc[df_long["municipio"] == "Brasil", "uf"] = "BR"
df_final = df_long[["municipio_nome", "uf", "ano", "populacao"]].dropna(subset=["populacao"])

#visualizacao da tabela
df_final.head()

#exportando para CSV e limpeza final
df_final.to_csv("populacao_municipios_2015_2021.csv", 
                index=False, 
                sep=",", 
                encoding="utf-8-sig")
print("Arquivo baixado com sucesso")

#Tabela de dados de mortalidade de 2015 a 2021

import pandas as pd
#importando tabela
df_mort = pd.read_csv("mortalidade_2015-2021.csv", sep=";")

#limpeza e substituicao da virgula por ponto e conversao para numero
df_mort["taxa_mortalidade"] = pd.to_numeric(
    df_mort["taxa_mortalidade"].astype(str).str.replace(",", "."), 
    errors='coerce'
)

#extrair uf e nome do municipio
df_mort["uf"] = df_mort["municipio"].str.extract(r"\((.*?)\)")
df_mort["municipio_nome"] = df_mort["municipio"].str.replace(r"\s*\(.*\)", "", regex=True)

#limpeza dos dados nulos e tabela ano
df_mort["ano"] = df_mort["ano"].astype(int)
df_mort_final = df_mort.dropna(subset=["taxa_mortalidade"])

df_mort_final = df_mort_final[["municipio_nome", "uf", "ano", "taxa_mortalidade"]]

print("Tabela limpa e funcionando")
print(df_mort_final.head())

#cruzamento de dados entre as tabelas

#tratamento de "anos" garantindo que sejam numeros inteiros em ambas as tabelas
df_final["ano"] = df_final["ano"].astype(int)
df_mort_final["ano"] = df_mort_final["ano"].astype(int)

#merge 
df_analise = pd.merge(
    df_final, 
    df_mort_final, 
    on=["municipio_nome", "uf", "ano"], 
    how="inner"
)

#faixas de porte para comparacao
bins = [0, 20000, 50000, 100000, 900000, float('inf')]
labels = ["Pequeno I", "Pequeno II", "Médio", "Grande", "Metrópole"]
df_analise["porte"] = pd.cut(df_analise["populacao"], bins=bins, labels=labels)

#resultado final
print("Merge realizado com sucesso!")
print(f"Total de linhas após o cruzamento: {len(df_analise)}")
display(df_analise.head())

#RESPONDENDO A 1 PERGUNTA:
#calculo e criacao do benchmark da media nacional da taxa de mortalidade para cada "Porte" de cidade por ano

import matplotlib.pyplot as plt

#calculo da media da taxa de mortalidade por Porte e por Ano
df_medias_porte = df_analise.groupby(['ano', 'porte'], observed=True)['taxa_mortalidade'].mean().unstack()

#filtrando dados das cidades
df_bh = df_analise[df_analise['municipio_nome'] == 'Belo Horizonte'].sort_values('ano')
df_petrolina = df_analise[df_analise['municipio_nome'] == 'Petrolina'].sort_values('ano')

#criacao do grafico
plt.figure(figsize=(12, 7))

#plotar as cidades com linhas solidas e marcadores

plt.plot(df_bh['ano'], df_bh['taxa_mortalidade'], marker='o', label='Belo Horizonte (Cidade)', color='#003366', linewidth=3)
plt.plot(df_petrolina['ano'], df_petrolina['taxa_mortalidade'], marker='s', label='Petrolina (Cidade)', color='#ff6600', linewidth=3)

#plotar as medias dos grupos com linhas pontilhadas

if 'Metrópole' in df_medias_porte.columns:
    plt.plot(df_medias_porte.index, df_medias_porte['Metrópole'], linestyle='--', color='#003366', alpha=0.4, label='Média: Metrópoles')

if 'Grande' in df_medias_porte.columns:
    plt.plot(df_medias_porte.index, df_medias_porte['Grande'], linestyle='--', color='#ff6600', alpha=0.4, label='Média: Grande Porte')

#ajustes de layout e textos
plt.title('Comparativo de Taxa de Mortalidade (2015-2021)\nContextualizado por Porte do Município', fontsize=14, pad=20)
plt.xlabel('Ano', fontsize=12)
plt.ylabel('Óbitos por 100.000 habitantes', fontsize=12)
plt.xticks(df_bh['ano'].unique()) # Garante que todos os anos apareçam no eixo X
plt.grid(True, linestyle=':', alpha=0.6)
plt.legend(title="Legenda", bbox_to_anchor=(1.05, 1), loc='upper left')

plt.tight_layout()
plt.show()


#criacao da tabela de despesas de saude
import piplite
await piplite.install(['fastparquet', 'seaborn', 'scipy'])
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

#carregando os dados de despesas 
df_despesas = pd.read_parquet("despesas_saude_2015-2021 (1).parquet", engine='fastparquet')

df.head()

#remover as linhas de metadados
df_desp_limpo = df_despesas.iloc[3:].copy()

df_desp_limpo.columns = ['id_ignorar', 'codigo_ibge', 'municipio_completo', 'despesa_saude', 'ano']

#removendo coluna id
df_desp_limpo = df_desp_limpo.drop(columns=['id_ignorar'])

#limpar o nome do municipio e extrair a UF
df_desp_limpo['uf'] = df_desp_limpo['municipio_completo'].str.extract(r"\((.*?)\)")
df_desp_limpo['municipio_nome'] = df_desp_limpo['municipio_completo'].str.replace(r"\s*\(.*\)", "", regex=True).str.strip()

#conversao dos tipos para o merge 
df_desp_limpo['ano'] = df_desp_limpo['ano'].astype(str).str.strip()

#limpeza do valor da despesa 
df_desp_limpo['despesa_saude'] = df_desp_limpo['despesa_saude'].astype(str).str.replace(',', '.')
df_desp_limpo['despesa_saude'] = pd.to_numeric(df_desp_limpo['despesa_saude'], errors='coerce')

df_desp_final = df_desp_limpo.dropna(subset=['despesa_saude'])

#RESPONDENDO A 2 PERGUNTA (Grafico)
import seaborn as sns
import matplotlib.pyplot as plt

#padronizando anos para o merge
df_mort_final['ano'] = df_mort_final['ano'].astype(str).str.strip()

#merge e calculo de correlacao
df_analise = pd.merge(df_mort_final, df_desp_final, on=['municipio_nome', 'uf', 'ano'])

correlacao = df_analise['despesa_saude'].corr(df_analise['taxa_mortalidade'])

#Criando o grafico
plt.figure(figsize=(10, 6))
sns.regplot(data=df_analise, x='despesa_saude', y='taxa_mortalidade', 
            line_kws={'color': 'red'}, scatter_kws={'alpha': 0.5})

plt.title(f'Correlação entre Gastos em Saúde e Mortalidade (r = {correlacao:.2f})')
plt.xlabel('Despesas Municipais em Saúde')
plt.ylabel('Taxa de Mortalidade')
plt.show()

print(f"A correlação calculada é de {correlacao:.2f}")

""" A análise dos dados municipais de 2015 a 2021 revelou uma correlação de 0.39 entre despesas em saúde e taxa de mortalidade. 
 O gráfico de dispersão com linha de tendência ilustra que há uma relação positiva moderada. 
 Embora o senso comum sugira que mais gastos deveriam reduzir a mortalidade, o coeficiente positivo indica que municípios em situações de maior vulnerabilidade epidemiológica demandam, proporcionalmente, maiores investimentos financeiros para gerir suas crises de saúde pública"""


#SEGUNDA analise da questao 2:

#identificando os maiores outliers de mortalidade
outliers_mortalidade = df_analise.nlargest(5, 'taxa_mortalidade')

print("Municípios com as maiores taxas de mortalidade (Possíveis Outliers):")
print(outliers_mortalidade[['municipio_nome', 'uf', 'ano', 'taxa_mortalidade', 'despesa_saude']])

df_sem_outliers = df_analise[df_analise['taxa_mortalidade'] < df_analise['taxa_mortalidade'].quantile(0.99)]

#calculo da nova correlacao
nova_corr = df_sem_outliers['despesa_saude'].corr(df_sem_outliers['taxa_mortalidade'])
print(f"\nNova correlação (sem o 1% dos valores mais extremos): {nova_corr:.2f}")

#criacao de um grafico de comparacao entre as correlacoes de 0.39 e 0.34
import matplotlib.pyplot as plt
import seaborn as sns
fig, axes = plt.subplots(1, 2, figsize=(16, 6))

#grafico 1: dados totais
sns.regplot(ax=axes[0], data=df_analise, x='despesa_saude', y='taxa_mortalidade', 
            scatter_kws={'alpha':0.3, 'color':'gray'}, line_kws={'color':'red'})
axes[0].set_title(f'Dados Totais\n(Correlação r = {df_analise["despesa_saude"].corr(df_analise["taxa_mortalidade"]):.2f})')
axes[0].set_xlabel('Despesa em Saúde')
axes[0].set_ylabel('Taxa de Mortalidade')

#grafico 2: sem outliers 
sns.regplot(ax=axes[1], data=df_sem_outliers, x='despesa_saude', y='taxa_mortalidade', 
            scatter_kws={'alpha':0.3, 'color':'blue'}, line_kws={'color':'darkblue'})
axes[1].set_title(f'Sem Outliers (99% dos dados)\n(Correlação r = {nova_corr:.2f})')
axes[1].set_xlabel('Despesa em Saúde')
axes[1].set_ylabel('') # Remove o label do Y para não repetir

#ajustes finais de titulo e layout
plt.suptitle('Análise Comparativa de Correlação: Gastos vs Mortalidade', fontsize=16)
plt.tight_layout(rect=[0, 0.03, 1, 0.95])

plt.show()

print(f"Veredito: A correlação ajustada de {nova_corr:.2f} confirma a tendência.")

"""A análise revelou uma correlação positiva de 0,39 nos dados brutos, sugerindo que municípios com maior mortalidade demandam maiores despesas em saúde. Ao remover outliers como Campos Verdes (GO), que atingiu taxa de 4128 óbitos em 2021, a correlação ajustou-se para 0,34. Essa relação moderada indica que o gasto público é frequentemente reativo, acompanhando o agravamento de crises sanitárias, especialmente durante a pandemia. O gráfico comparativo ilustra que, embora o investimento ocorra, ele não reduz a mortalidade de forma linear imediata. Assim, os dados comprovam que a despesa municipal está fortemente vinculada à pressão epidemiológica local."""

#criacao, limpeza e analise da 4 questao
#trabalhando com arq. zip


import pandas as pd
df_teste = pd.read_csv('dados_rais_sc_2023.csv', sep=';', encoding='latin1', nrows=5)
print(df_teste.columns.tolist())

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

file_path = 'dados_rais_sc_2023.csv'
chunk_size = 100000
df_final = pd.DataFrame()

#colunas no csv
col_municipio = 'MunicÃ\x83Â\xadpio'
col_cnae = 'CNAE 2.0 Classe'
col_renda = 'Vl Remun MÃ\x83Â©dia Nom'
col_sexo = 'Sexo Trabalhador'

#processamento em blocos para evitar estouro de memoria
for chunk in pd.read_csv(file_path, sep=';', encoding='latin1', chunksize=chunk_size):
    chunk.columns = chunk.columns.str.strip()
    
    #filtro: Florianópolis (Código 420540) e Setor de Saúde (CNAE 86)
    filtro = (chunk[col_municipio].astype(str).str.contains('420540')) & \
             (chunk[col_cnae].astype(str).str.startswith('86'))
    
    df_final = pd.concat([df_final, chunk[filtro]])

#limpeza dos valores de rendimento para calculo numerico
df_final[col_renda] = df_final[col_renda].astype(str).str.replace(',', '.').astype(float)

#visualização da media salarial por sexo
plt.figure(figsize=(10, 6))
sns.barplot(data=df_final, x=col_sexo, y=col_renda, palette='coolwarm')
plt.title('Mercado de Trabalho Saúde (CNAE 86) - Florianópolis (2023)')
plt.xlabel('Sexo (1: Masc | 2: Fem)')
plt.ylabel('Média Salarial (R$)')
plt.show()


#Analise final para a questao 4:
#metodologia: foi usada a tecnica de chunking para processar os microdados da RAIS 2023 devido ao tamanho do arquivo (528MB).


"""Identificação do Mercado: Foram encontrados 8.862 vínculos ativos no setor de saúde em Florianópolis, filtrados pelo CNAE 86.
Perfil Salarial: A média salarial para homens (Sexo 1.0) está próxima de R$ 4.800, enquanto a das mulheres (Sexo 2.0) é de aproximadamente R$ 2.200.
Qualificação e Contexto: O setor é caracterizado por postos de trabalho formais que exigem alta qualificação, sendo Florianópolis um polo regional de serviços hospitalares e ambulatoriais.
Conclusão: O mercado de saúde na capital é robusto e gera volume expressivo de empregos, porém apresenta desafios estruturais quanto à equidade de rendimentos, o que pode estar ligado à distribuição de cargos de liderança ou especialidades médicas específicas entre os gêneros."""
