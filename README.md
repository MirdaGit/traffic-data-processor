# Traffic data processor

Nástroj slouží k automatickému získávání a zpracování dat dopravních nehod pro vybranou oblast. Je vysoce konfigurovatelný, takže lze jeho funkcionalitu snadno přizpůsobit vlastním požadavkům. Díky modulárnímu návrhu jej lze navíc rozšířit o získání a zpracování dalších zdrojů dat bez zásahu do stávající funkcionality. Nástroj je využíván městem Most, vizualizovaná data je možné vidět [zde](https://mapy.mesto-most.cz/app/dopravni-udalosti/).

## Automatizované získávání a zpracování dat

Získání a zpracování dat zajišťuje Python skript [process_data](./process_data.py), který využívá třídy a jejich metody z následujících modulů:

| Modul             | Funkce                                                        |
| ----------------- | ------------------------------------------------------------- |
| scrapers.py       | Automatické získávání dat z webových stránek a jejich uložení |
| api_extractors.py | Získávání a zpracování dat ze specifikovaných API zdrojů      |
| api_loaders.py    | Ukládání dat z API zdrojů                                     |
| extractors.py     | Zpracování lokálně uložených dat                              |
| geofilters.py     | Filtrování dat na základě prostorových údajů                  |
| loaders.py        | Ukládání zpracovaných dat                                     |

Každý z těchto modulů obsahuje základní abstraktní třídu, ze které dědí všechny konkrétní třídy. Výběr použité třídy probíhá na základě konfigurace, což umožňuje například odlišné zpracování souborů různých typů nebo využití odlišných knihoven. Tato flexibilita je klíčová pro případy, kdy uživatel nevlastní licenci ArcGIS, a nemůže tak využít knihovnu ArcPy. V takových případech je možné využít alternativních metod, které jsou implementovány pomocí knihovny GeoPandas, která licenci nevyžaduje.

Konfigurace skriptu se nachází v souboru [config.json](./config.json) a její popis včetně očekávaných hodnot je popsán v souboru [schema.json](./schema.json).

## Začínáme

### Prerekvizity

Funkčnost skriptů pro získávání a zpracování dat byla otestována s následujícími verzemi nástrojů:

| Nástroj | Verze   |
| ------- | ------- |
| Python  | 3.11.10 |
| pip     | 23.3.2  |
| conda   | 24.5.0  |

V případě využití licencovaných nástrojů ArcGIS byly otestovány tyto verze:

| Nástroj           | Verze |
| ----------------- | ----- |
| ArcGIS Pro        | 3.4.2 |
| ArcGIS Enterprise | 10.9  |

Mimo nástroje je navíc **nutné** dodat polygon, pomocí kterého bude prováděno filtrování prostorových dat. Pro vyzkoušení je možné využít ukázkových polygonů obcí s rozšířenou působností a městských částí města Brna, které se nachází ve složce [examples](./examples/). Související prvky konfigurace jsou popsány [zde](#přehled-hlavních-prvků-konfigurace) nebo podrobněji v souboru [schema.json](./schema.json).

### Instalace bez následovného využití ArcGIS

1. Naklonování repozitáře

    ```sh
    https://github.com/MirdaGit/traffic-data-processor
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

### Instalace s následovným využitím ArcGIS

1.  Naklonování repozitáře
    ```sh
    https://github.com/MirdaGit/traffic-data-processor
    ```
2.  Nastavení prostředí

    a) Vytvoření kopie základního prostředí `arcgispro-py3` (obecně prostředí, které obsahuje knihovnu ArcPy)

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

V základní podobě jsou po spuštění automaticky staženy archivy dopravních nehod ze stránek Policie ČR a následně jsou rozbaleny do složky `data`. V rámci zpracování jsou tyto soubory postupně nahrazeny soubory typu Pickle. Tímto je při opakovaném spuštění zajištěno vyrazně rychlejší načítání dat a současně dochází i ke snížení nároků na úložiště. Průběh programu je zaznamenáván do souboru `logs.txt`. Rozsah výpisů se odvíjí od nastavené úrovně logování v konfiguračním souboru.

### Přehled hlavních prvků konfigurace

Pro snažší orientaci v konfiguračním souboru jsou zde popsány jeho nejdůležitější prvky:

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
				Určení místa uložení získaných dat.
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
				Konfigurace pro načtení polygonu, podle kterého budou filtrována prostorová data.
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
				Po stažení jsou zachovány pouze zde definované soubory. Pro starší data dopravních nehod <b>je nutné upravit všechna čísla kraje v konfiguraci</b> podle čísleníku krajů Policie ČR.
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
				Související soubory jsou uchovány ve stejné složce a jsou zpracovány společně (např. data nehod a zúčastněných vozidel) – odděleně nebudou zpracovány správně. Alespoň jeden ze souborů ve složce <b>musí obsahovat polohové údaje</b>.
			</td>
        </tr>
    </tbody>
</table>

### (Volitelné) Doplňující skripty Arcade pro výpočet dodatečných polí

Ve složce [arcade](./arcade/) se nachází dva skripty v jazyce Arcade, které je možné využít v rámci ArcGIS geodatabáze. Tyto skripty se vkládají do atributových pravidel třídy prvků dopravních nehod. Při každé aktualizaci prvků automaticky vyhodnocují počty nehod a další statistiky pro vrstvy silnic a dopravních kamer. Výpočet je při importu dat do geodatabáze proveden pouze jednou, což omezuje negativní dopad na celkovou rychlost zpracování.

Jedná se pouze o dodatečné zpracování, které **není pro běh skriptu potřebné**. Při případném využití těchto skriptů je do projektu nutné přidat relevantní vrstvy silnic nebo dopravních kamer a upravit cesty k třídám prvků, které jsou v nich nastavené.

## Licence

Tento projekt je licencován pod licencí MIT - podrobnosti najdete v souboru [LICENSE](./LICENSE).

## Poděkování

Tento nástroj vznikl jako součást diplomové práce [„Aplikace pro analýzu dopravních událostí ve městě Most“](https://www.vut.cz/studenti/zav-prace/detail/164723) na Fakultě informačních technologií Vysokého učení technického v Brně. Práce byla vypracována pod vedením Ing. Jiřího Hynka Ph.D. a konzultována s referentem města Most Ing. Kamilem Novákem. Oběma děkuji za pomoc a cenné rady při tvorbě této práce.
