# Spolkový Hospodský Systém 🍺

Webová POS aplikace pro správu baru/hospody vesnických spolků.

## Rychlý start (lokální vývoj)

```bash
cd spolek-hub
python -m venv .venv
.venv\Scripts\activate          # Windows
pip install -r requirements.txt
cp .env.example .env            # upravte dle potřeby
streamlit run app.py
```

Přihlášení: `admin` / `admin123`

## Google Drive setup

1. V Google Cloud Console vytvořte Service Account
2. Stáhněte JSON klíč
3. Na Google Drive vytvořte složku a sdílejte ji se Service Account (Editor)
4. Zkopírujte Folder ID ze URL
5. Nastavte v `.env`:
   ```
   GDRIVE_FOLDER_ID=<folder_id>
   GDRIVE_CREDENTIALS_JSON=<obsah JSON souboru jako string>
   ```

## Nasazení na Streamlit Cloud

1. Pushněte repo na GitHub (bez `.env` a databázového souboru)
2. Na share.streamlit.io připojte repo, main file: `app.py`
3. V sekci **Secrets** přidejte:
   ```toml
   GDRIVE_FOLDER_ID = "..."
   GDRIVE_CREDENTIALS_JSON = '{"type":"service_account",...}'
   BASE_URL = "https://your-app.streamlit.app"
   ```

## Nasazení na Android (Chrome PWA)

1. Otevřete app URL v Chrome
2. Menu → "Přidat na plochu"
3. Spusťte jako PWA – fullscreen, bez adresního řádku

## Účty a způsoby platby

| Režim | Popis |
|-------|-------|
| `immediate` | Každá objednávka se platí ihned po podání |
| `tab` | Objednávky se hromadí, platba při uzavření účtu |

## Zákaznické objednávky – jak aktivovat

1. Admin → Zákazníci → detail zákazníka
2. Sekce "Oprávnění" → zapněte "Smí objednávat přes app"
3. Volitelně: "Smí mít otevřený účet" + kreditní limit
4. Zákazník obdrží QR kód s osobním tokenem

## QR kódy

- **Osobní QR** (`?uid=xyz`): identifikuje zákazníka, přístup k profilu a objednávání
- **Stolový QR** (`?table=xyz`): anonymní objednávky ke stolu
- **Kombinovaný** (`?uid=xyz&table=abc`): zákazník + stůl

## Backup a obnova databáze

- Automatická denní záloha na Google Drive (uchováváno 7 dní)
- Soubory: `spolek_pos.db`, `spolek_pos_backup_YYYY-MM-DD.db`
- Obnova: nahraďte soubor v GDrive a restartujte aplikaci
