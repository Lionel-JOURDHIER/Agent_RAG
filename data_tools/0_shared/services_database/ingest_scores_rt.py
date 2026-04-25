import sys
from pathlib import Path

# Ajoute 0_shared/ au path pour trouver init_db, config, tables/
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import Config_bdd
from init_db import get_engine, get_session_factory
from services_database.build_scores_rt import build_scores_rt
from tables.films import Film
from tables.scores_rt import ScoreRt


def ingest_scores_rt_pipeline():
    """
    Orchestrates the ingestion of Rotten Tomatoes scores with strict integrity
    checks and manual type sanitization for database compatibility.

    Returns:
        pd.DataFrame: The processed DataFrame after filtering and cleaning.
    """
    # 1. Generate the cleaned DataFrame from source
    df = build_scores_rt()

    # Initialize DB connection components
    engine = get_engine(Config_bdd.DATABASE_URL)
    SessionFactory = get_session_factory(engine)

    # 3. Insertion
    with SessionFactory() as session:
        try:
            print(f"Début de l'insertion dans {Config_bdd.DATABASE_URL}...")

            ## --- PRE-FILTERING: Referential Integrity ---
            # Extract non-null tertiary IDs from the current batch
            candidates = [v for v in df["id_tertiaire"].tolist() if v is not None]

            # Only query the DB for candidates present in the current dataframe
            valid_tertiaire = {
                r[0]
                for r in session.query(Film.id_tertiaire)
                .filter(Film.id_tertiaire.in_(candidates))
                .all()
            }

            # Identify and log orphaned records (IDs not found in Film table)
            orphan_count = len(df) - df["id_tertiaire"].isin(valid_tertiaire).sum()
            if orphan_count:
                print(
                    f"⚠️  {orphan_count} lignes ignorées (id_tertiaire absent de films)"
                )
            # Filter the dataframe to keep only valid foreign keys
            df = df[df["id_tertiaire"].isin(valid_tertiaire)]

            # --- DEDUPLICATION: Check existing records ---
            existing_id_score_rt = {
                r[0] for r in session.query(ScoreRt.id_tertiaire).all()
            }

            count_added = 0

            # --- DATA SANITIZATION ---
            # Manual conversion to ensure pure Python types (int/None) before insertion
            # to bypass Pandas/NumPy type inference issues.
            df["rt_tomatometer"] = [
                int(x) if x is not None and str(x) != "nan" else None
                for x in df["rt_tomatometer"]
            ]
            df["rt_audience_score"] = [
                int(x) if x is not None and str(x) != "nan" else None
                for x in df["rt_audience_score"]
            ]
            df["rt_critics_consensus"] = [
                None if str(x) == "nan" else x for x in df["rt_critics_consensus"]
            ]

            for _, row in df.iterrows():
                # Clean row items: detect NaN objects where (v != v) is True
                clean = {
                    k: (None if isinstance(v, float) and v != v else v)
                    for k, v in row.items()
                }

                id_tertiaire = clean["id_tertiaire"]
                # Insert only if ID is valid and not already in destination table
                if id_tertiaire and id_tertiaire not in existing_id_score_rt:
                    session.add(
                        ScoreRt(
                            id_tertiaire=id_tertiaire,
                            url_rotten=clean["url_rotten"],
                            rt_tomatometer=int(clean["rt_tomatometer"])
                            if clean["rt_tomatometer"] is not None
                            else None,
                            rt_audience_score=int(clean["rt_audience_score"])
                            if clean["rt_audience_score"] is not None
                            else None,
                            rt_critics_consensus=clean["rt_critics_consensus"],
                        )
                    )
                    # Track newly added ID to prevent duplicates within the same source
                    existing_id_score_rt.add(id_tertiaire)
                    count_added += 1

            # Commit the transaction to the database
            session.commit()
            print(
                f"✅ Migration terminée : {count_added} nouveaux scores_rt importées."
            )

        except Exception as e:
            # Revert all changes in this session if any error occurs
            session.rollback()
            print(f"❌ Erreur lors de l'ingestion : {e}")
    return df


if __name__ == "__main__":
    ingest_scores_rt_pipeline()
