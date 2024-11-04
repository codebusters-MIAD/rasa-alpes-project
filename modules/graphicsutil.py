#Used Libraries
import pandas as pd
from pyspark.sql.types import NumericType
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import pearsonr

def plot_numeric_box_plots(spark_df, sample_fraction=0.1):
    """
    Genera diagramas de caja para todas las columnas numéricas de un DataFrame de Spark.

    Parámetros:
    - spark_df: DataFrame de Spark.
    - sample_fraction: Fracción de datos a muestrear (valor entre 0 y 1). Por defecto es 1.0 (todos los datos).
    Retorna:
    - None. La función muestra los diagramas de caja.
    """
    # Identificar las columnas numéricas
    numeric_columns = [field.name for field in spark_df.schema.fields if isinstance(field.dataType, NumericType)]

    if not numeric_columns:
        print("El DataFrame no contiene columnas numéricas.")
        return

    if sample_fraction < 1.0:
        spark_df = spark_df.sample(fraction=sample_fraction)

    spark_numeric_df = spark_df.select(*numeric_columns)

    pandas_df = spark_numeric_df.toPandas()
    pandas_df = pandas_df.dropna()

    sns.set(style="whitegrid")

    num_columns = len(numeric_columns)
    fig, axes = plt.subplots(nrows=1, ncols=num_columns, figsize=(5 * num_columns, 6))

    if num_columns == 1:
        axes = [axes]

    for ax, column in zip(axes, numeric_columns):
        sns.boxplot(y=pandas_df[column], ax=ax)
        ax.set_title(f'Diagrama de caja de {column}')
        ax.set_ylabel(column)
        ax.set_xlabel('')

    plt.tight_layout()
    plt.show()

def plot_correlation_heatmap(spark_df, sample_fraction=1.0):
    """
    Genera un mapa de calor de correlaciones para todas las columnas numéricas de un DataFrame de Spark.
    Cuando los valores son demasiado altos puede afectar el resultado del diagrama

    Parámetros:
    - spark_df: DataFrame de Spark.
    - sample_fraction: Fracción de datos a muestrear (valor entre 0 y 1). Por defecto es 1.0 (todos los datos).

    Retorna:
    - None. La función muestra el mapa de calor de correlaciones.
    """
    numeric_columns = [field.name for field in spark_df.schema.fields if isinstance(field.dataType, NumericType)]

    if not numeric_columns:
        print("El DataFrame no contiene columnas numéricas.")
        return

    if sample_fraction < 1.0:
        spark_df = spark_df.sample(fraction=sample_fraction)

    spark_numeric_df = spark_df.select(*numeric_columns)

    pandas_df = spark_numeric_df.toPandas()

    pandas_df = pandas_df.dropna()

    corr_matrix = pandas_df.corr()

    plt.figure(figsize=(10, 8))
    sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', fmt='.2f', linewidths=0.5)
    plt.title('Mapa de calor de correlaciones')
    plt.show()


def plot_scatter(df, x_col, y_col, sample_fraction):
    """
    Genera un gráfico de dispersión entre dos columnas de un DataFrame de Spark.

    Parámetros:
    df (DataFrame): DataFrame de Spark con los datos.
    x_col (str): Nombre de la columna para el eje x.
    y_col (str): Nombre de la columna para el eje y.
    sample_fraction (float): Porcentaje de datos a muestrear (entre 0 y 1).
    """

    if sample_fraction <= 0 or sample_fraction > 1:
        raise ValueError("El parámetro sample_fraction debe estar entre 0 y 1.")

    sample_df = df.select(x_col, y_col).sample(fraction=sample_fraction)

    pandas_df = sample_df.toPandas()

    if pandas_df.empty:
        print("No hay datos para visualizar después del muestreo. Por favor, incrementa sample_fraction.")
        return

    if pandas_df[x_col].dtype == 'object':
        try:
            pandas_df[x_col] = pd.to_datetime(pandas_df[x_col])
        except Exception as e:
            print(f"Error al convertir {x_col} a datetime: {e}")
            return

    plt.figure(figsize=(12, 6))
    sns.scatterplot(data=pandas_df, x=x_col, y=y_col)
    plt.title(f"Gráfico de dispersión de {y_col} vs {x_col}")
    plt.xlabel(x_col)
    plt.ylabel(y_col)
    plt.xticks(rotation=45)  # Rotar etiquetas del eje x si es necesario
    plt.tight_layout()
    plt.show()



# %%
def plot_pairplot_with_corr(spark_df, numeric_columns):
    """
    Genera un gráfico de pares con coeficientes de correlación para las columnas numéricas de un DataFrame de Spark.

    Parámetros:
    - spark_df: DataFrame de Spark.
    - numeric_columns: Lista de columnas numéricas a utilizar para el gráfico de pares.

    Retorna:
    - None. La función muestra el gráfico de pares.
    """
    # Convertir las columnas numéricas del DataFrame de Spark a un DataFrame de Pandas
    df_nums = spark_df.select(numeric_columns).toPandas()

    def corrfunc(x, y, **kws):
        r, _ = pearsonr(x, y)
        ax = plt.gca()
        ax.annotate(f"r = {r:.2f}", xy=(0.5, 0.1), xycoords=ax.transAxes, ha='center', fontsize=10)

    # Crear el gráfico de pares con seaborn
    sns.pairplot(df_nums, diag_kind="kde", kind="reg", plot_kws={'line_kws': {'color': 'red'}},
                 markers="+").map_lower(corrfunc)

    plt.show()