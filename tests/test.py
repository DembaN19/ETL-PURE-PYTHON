fmt_path = "/tmp/Vat_Entry_20250716_112438.fmt"

with open(fmt_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Nombre de colonnes (ligne 2)
expected_column_count = int(lines[1].strip())

# Lignes des colonnes
column_lines = lines[2:2 + expected_column_count]

# Extraire l'avant-dernier champ non vide de chaque ligne (le nom de colonne)
column_names = [line.strip().split()[-2] for line in column_lines]

print(column_names)


def verify_and_fix_format_file(fmt_path, expected_columns, list_csv_columns):
    with open(fmt_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # Nombre de colonnes (ligne 2)
    actual_columns = int(lines[1].strip())

    # Lignes des colonnes
    column_lines = lines[2:2 + actual_columns]

    # Extraire l'avant-dernier champ non vide de chaque ligne (le nom de colonne)
    column_names = [line.strip().split()[-2] for line in column_lines]

    # afficher les colonnes manquantes 
    missing_columns = [col for col in list_csv_columns if col not in column_names]
    if missing_columns:
        print(f"❌ Missing columns in format file: {missing_columns}")
        
    if actual_columns != expected_columns:
        raise ValueError(f"❌ Format file column count mismatch: {actual_columns} in .fmt vs {expected_columns} in CSV")

    # Vérifie et corrige le terminator de la dernière ligne
    last_line = lines[-1]
    if '"\\n"' in last_line:
        corrected_last_line = last_line.replace('"\\n"', '"\\r\\n"')
        lines[-1] = corrected_last_line
        with open(fmt_path, 'w', encoding='utf-8') as f:
            f.writelines(lines)
        print(f"✅ Corrected line terminator in format file: '\\n' → '\\r\\n'")
    else:
        print(f"✅ Format file already has correct line terminator.")

