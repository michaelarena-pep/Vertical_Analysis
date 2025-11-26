import csv
import pathlib
import tempfile
import os

def main():
	INPUT_CSV = 'data/Website_comp_info_company_type_v2.csv'
	SUB_VERTICAL_COL = 'Sub Vertical'
	input_path = pathlib.Path(INPUT_CSV)
	if not input_path.exists():
		print(f"ERROR: Input CSV not found at {INPUT_CSV}")
		return

	with open(input_path, newline='', encoding='utf-8') as f:
		reader = csv.DictReader(f)
		fieldnames = list(reader.fieldnames or [])
		rows = list(reader)

	changed = False
	for row in rows:
		val = (row.get(SUB_VERTICAL_COL) or '').strip()
		if val.startswith('ERROR:'):
			row[SUB_VERTICAL_COL] = ''
			changed = True

	if changed:
		tmp_fd, tmp_path = tempfile.mkstemp(prefix='tmp_clean_', suffix='.csv', dir=str(input_path.parent))
		os.close(tmp_fd)
		with open(tmp_path, 'w', newline='', encoding='utf-8') as f:
			writer = csv.DictWriter(f, fieldnames=fieldnames)
			writer.writeheader()
			writer.writerows(rows)
		os.replace(tmp_path, str(input_path))
		print(f"Blanked out 'ERROR:' values in {input_path}")
	else:
		print("No 'ERROR:' values found to blank out.")

if __name__ == '__main__':
	main()
