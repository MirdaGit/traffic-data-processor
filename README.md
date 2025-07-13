# Traffic data processor

Nástroj slouží k automatickému získávání a zpracování dat dopravních nehod pro určenou oblast. Je vysoce konfigurovatelný, takže jeho funkcionalitu lze snadno přizpůsobit vlastním požadavkům. Řešení je navíc navrženo tak, aby jej bylo možné jednodušše rozšiřovat bez zásahu do stávající funkcionality.

## Automatizované získání a zpracování dat

Získání a zpracování dat zajišťuje Python skript [process_data](./process_data.py), který při svém běhu využívá třídy a jejich metody z následujících modulů:

| Modul             | Popis obsažených tříd                                                    |
| ----------------- | ------------------------------------------------------------------------ |
| scrapers.py       | Automatické získávání dopravních dat z webových stránek a jejich uložení |
| api_extractors.py | Získávání a zpracování dat ze specifikovaných API zdrojů                 |
| api_loaders.py    | Ukládání dat z API zdrojů                                                |
| extractors.py     | Zpracování lokálně uložených dat                                         |
| geofilters.py     | Filtrování dat na základě prostorových údajů                             |
| loaders.py        | Ukládání zpracovaných dat                                                |

Každý z těchto modulů obsahuje základní abstraktní třídu, ze které dědí všechny konkrétní třídy. Jejich výběr probíhá na základě konfigurace, což umožňuje například odlišné zpracování souborů různých typů nebo použití odlišných knihoven. Tato flexibilita je klíčová pro případy, kdy uživatel nevlastní licenci ArcGIS, a nemůže tak využít knihovnu ArcPy. V takových případech je možné využít alternativních metod, které jsou implementovány pomocí knihovny GeoPandas, která licenci nevyžaduje.

Konfigurace skriptu se nachází v souboru [config.json](./config.json) a její struktura včetně očekávaných hodnot je popsána v souboru [schema.json](./schema.json).

## Začínáme

### Prerekvizity

Funkčnost skriptů pro získávání a zpracování dat byla otestována s následujícími verzemi nástrojů:

| Nástroj | Verze   |
| ------- | ------- |
| Python  | 3.11.10 |
| pip     | 23.3.2  |

V případě využití licencovaných nástrojů ArcGIS byly otestovány tyto verze:

| Nástroj           | Verze  |
| ----------------- | ------ |
| conda             | 24.5.0 |
| ArcGIS Pro        | 3.4.2  |
| ArcGIS Enterprise | 10.9   |

Mimo samotné nástroje je navíc **nutné** mít k dipozici polygon, pomocí kterého bude prováděno filtrování prostorových dat. Související prvky konfigurace jsou popsány [zde](#přehled-hlavních-prvků-konfigurace).

### Instalace bez využití ArcGIS

1. Naklonování repozitáře

    ```sh
    https://github.com/MirdaGit/traffic-data-visualizer
    ```

2. _(Volitelné)_ Nastavení prostředí

    a) Vytvoření základního prostředí

    ```sh
    conda create --name "jméno_prostředí"
    ```

    b) _(Volitelné)_ Inicializace conda a **restartování terminálu**

    ```sh
    conda init
    ```

    c) Aktivace vytvořeného prostředí

    ```sh
    conda activate "jméno_prostředí"
    ```

3. Instalace požadovaných knihoven
    ```sh
    pip install -r requirements.txt
    ```

### Instalace s využitím ArcGIS

1.  Naklonování repozitáře
    ```sh
    https://github.com/MirdaGit/traffic-data-visualizer
    ```
2.  Nastavení prostředí

    a) Vytvoření kopie základního prostředí `arcgispro-py3`

    ```sh
    conda create --clone arcgispro-py3 --name "jméno_prostředí"
    ```

    b) _(Volitelné)_ Inicializace conda a **restartování terminálu**

    ```sh
    conda init
    ```

    c) Aktivace vytvořeného prostředí

    ```sh
    conda activate "jméno_prostředí"
    ```

3.  Instalace požadovaných knihoven
    ```sh
    pip install -r requirements.txt
    ```

### Spuštění skriptu

```sh
python process_data.py
```

V základní podobě jsou po spuštění automaticky staženy archivy dopravních nehod ze stránek Policie ČR a jsou rozbaleny do složky `data`. V rámci zpracování jsou tyto soubory postupně nahrazeny soubory ve formátu Pickle. Tímto je při opakovaném spuštění zajištěno vyrazně rychlejší načítání dat a současně dochází i ke snížení nároků na úložiště. Průběh programu je zaznamenáván do souboru `logs.txt`. Rozsah výpisů se odvíjí od nastavené úrovně logování.

### Přehled hlavních prvků konfigurace

Pro snažší orientaci v konfiguračním souboru jsou níže uvedeny a popsány jeho nejdůležitější prvky:

<table>
    <thead>
        <tr>
            <th>Prvek konfigurace</th>
            <th>Popis</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>data_folder</td>
            <td>
				Místo uložení dat, která mají být zpracována.<br/>
			</td>
        </tr>
		<tr>
            <td>logs</td>
            <td>
				Konfigurace logování běhu programu.<br/>
			</td>
        </tr>
        <tr>
			<td rowspan=3>scrapers</td>
            <td>
				Seznam cílů pro scraping a jejich konfigurace.
			</td>
        </tr>
		<tr>
            <td>
				Seznam může být prázdný – žádná data nebudou stažena z webových stránek.
			</td>
        </tr>
		<tr>
            <td>
				Místo uložení automaticky stažených dat.
			</td>
        </tr>
		<tr>
			<td rowspan=2>apis</td>
            <td>
				Seznam zdrojů API a jejich konfigurace.
			</td>
        </tr>
		<tr>
            <td>
				Seznam může být prázdný – žádná data z API zdrojů nebudou zpracována.
			</td>
        </tr>
		<tr>
			<td>polygon_filter</td>
            <td>
				Konfigurace pro načtení polygonu, podle kterého jsou prostorová data filtrována.
			</td>
        </tr>
		<tr>
			<td>loaders</td>
            <td>
				Konfigurace výstupu a mapování vstupních souborů na výstup.
			</td>
        </tr>
		<tr>
			<td rowspan=4>data_files</td>
            <td>
				Po stažení jsou zachovány pouze zde definované soubory. Pro starší data dopravních nehod je nutné upravit všechna čísla kraje v konfiguraci podle čísleníku krajů Policie ČR.
			</td>
        </tr>
		<tr>
            <td>
				Konfigurace zpracování a uložení jednotlivých vstupních souborů.
			</td>
        </tr>
		<tr>
            <td>
				Zpracovány jsou pouze zde uvedené soubory, ostatní jsou ignorovány.
			</td>
        </tr>
		<tr>
            <td>
				Související soubory jsou uchovány ve stejné složce a jsou zpracovány společně – odděleně nebudou zpracovány správně. Alespoň jeden ze souborů ve složce musí obsahovat polohové údaje.
			</td>
        </tr>
    </tbody>
</table>

### (Volitelné) Doplňující skripty Arcade pro výpočet dodatečných polí

Ve složce [arcade](./arcade/) se nachází dva skripty v jazyce Arcade, které je možné využít v rámci ArcGIS geodatabáze. Tyto skripty se vkládají do atributových pravidel třídy prvků dopravních nehod. Při každé aktualizaci prvků automaticky vyhodnocují počty nehod a další statistiky pro vrstvy silnic a dopravních kamer. Výpočet je při importu dat do geodatabáze proveden pouze jednorázově, a proto má pouze omezenější dopad na celkovou rychlost zpracování.

Jedná se pouze o dodatečné zpracování, které **není pro běh skriptu potřebné**. Pro případné využití těchto skriptů je do projektu nutné přidat relevantní vrstvy silnic nebo dopravních kamer a upravit cesty, které jsou v nich pevně nastavené.

## Licence

Tento projekt je licencován pod licencí MIT - podrobnosti najdete v souboru [LICENSE](./LICENSE).

## Poděkování

Tento nástroj vznikl jako součást diplomové práce „Aplikace pro analýzu dopravních událostí ve městě Most“ na Fakultě informačních technologií Vysokého učení technického v Brně. Práce vypracována pod vedením Ing. Jiřího Hynka Ph.D. a konzultována s referentem města Most Ing. Kamilem Novákem. Oběma děkuji cenné rady při tvorbě této práce.
