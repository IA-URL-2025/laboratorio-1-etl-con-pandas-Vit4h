import pandas as pd


def run_etl():
    """
    Implementa el proceso ETL.
    No cambies el nombre de esta función.
    """
    input_path = "data/citas_clinica.csv"
    output_path = "data/output.csv"

    # EXTRACT
    df = pd.read_csv(input_path)

    # TRANSFORM

    # 1) Normalización de texto
    df["paciente"] = df["paciente"].astype("string")
    df.loc[df["paciente"].notna(), "paciente"] = df.loc[df["paciente"].notna(), "paciente"].str.strip().str.title()

    # especialidad -> UPPERCASE
    df["especialidad"] = df["especialidad"].astype("string")
    df.loc[df["especialidad"].notna(), "especialidad"] = df.loc[df["especialidad"].notna(), "especialidad"].str.strip().str.upper()

    # 2) Fechas: convertir y filtrar inválidas
    df["fecha_cita"] = pd.to_datetime(df["fecha_cita"], errors="coerce")
    df = df[df["fecha_cita"].notna()].copy()

    # 3) Reglas de negocio
    df["estado"] = df["estado"].astype("string").str.strip()
    df["costo"] = pd.to_numeric(df["costo"], errors="coerce")
    df = df[(df["estado"] == "CONFIRMADA") & (df["costo"] > 0)].copy()

    # 4) Nulos en telefono
    df["telefono"] = df["telefono"].astype("string")
    df["telefono"] = df["telefono"].fillna("NO REGISTRA")
    # también por si viene como string vacío (espacios)
    df.loc[df["telefono"].str.strip() == "", "telefono"] = "NO REGISTRA"

    df["fecha_cita"] = df["fecha_cita"].dt.date

    # LOAD
    df.to_csv(output_path, index=False)


if __name__ == "__main__":
    run_etl()

