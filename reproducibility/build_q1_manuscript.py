from __future__ import annotations

import argparse
import copy
import csv
import json
import re
import tempfile
import zipfile
from pathlib import Path

from lxml import etree
from PIL import Image


W_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
M_NS = "http://schemas.openxmlformats.org/officeDocument/2006/math"
WP_NS = "http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing"
A_NS = "http://schemas.openxmlformats.org/drawingml/2006/main"
DC_NS = "http://purl.org/dc/elements/1.1/"
XML_NS = "http://www.w3.org/XML/1998/namespace"
NS = {"w": W_NS, "m": M_NS, "wp": WP_NS, "a": A_NS, "dc": DC_NS}
W = f"{{{W_NS}}}"

NEW_TITLE = (
    "Climate and Aggregate Human-Activity Scenario Contrasts in Flood and Drought "
    "Event Frequencies across China (1950–2019): An ISIMIP3a Multi-Model Analysis"
)


def p_text(node: etree._Element) -> str:
    parts: list[str] = []
    for el in node.iter():
        if el.tag == W + "t" and el.text:
            parts.append(el.text)
        elif el.tag == W + "tab":
            parts.append("\t")
        elif el.tag in {W + "br", W + "cr"}:
            parts.append("\n")
    return "".join(parts)


def run(text: str, rpr: etree._Element | None = None) -> etree._Element:
    r = etree.Element(W + "r")
    if rpr is not None:
        r.append(copy.deepcopy(rpr))
    t = etree.SubElement(r, W + "t")
    if text.startswith(" ") or text.endswith(" "):
        t.set(f"{{{XML_NS}}}space", "preserve")
    t.text = text
    return r


def clone_plain(template: etree._Element, text: str) -> etree._Element:
    p = etree.Element(W + "p", nsmap=template.nsmap)
    ppr = template.find("w:pPr", namespaces=NS)
    if ppr is not None:
        p.append(copy.deepcopy(ppr))
    p.append(run(text, template.find("w:r/w:rPr", namespaces=NS)))
    return p


def set_plain(p: etree._Element, text: str) -> None:
    if p.xpath(".//w:fldChar | .//w:drawing | .//m:oMath", namespaces=NS):
        raise ValueError(f"Refusing to replace non-plain paragraph: {p_text(p)[:100]}")
    ppr = p.find("w:pPr", namespaces=NS)
    rpr = p.find("w:r/w:rPr", namespaces=NS)
    for child in list(p):
        if child is not ppr:
            p.remove(child)
    p.append(run(text, rpr))


def replace_text_nodes(root: etree._Element, old: str, new: str, expected: int = 1) -> None:
    count = 0
    for text_node in root.xpath(".//w:t", namespaces=NS):
        if text_node.text and old in text_node.text:
            text_node.text = text_node.text.replace(old, new)
            count += 1
    if count != expected:
        raise RuntimeError(f"Expected {expected} occurrence(s) of {old!r}, found {count}")


def find_exact(body: etree._Element, text: str) -> etree._Element:
    for p in body.xpath("./w:p", namespaces=NS):
        if p_text(p).strip() == text:
            return p
    raise KeyError(text)


def find_start(body: etree._Element, prefix: str) -> etree._Element:
    matches = [p for p in body.xpath("./w:p", namespaces=NS) if p_text(p).strip().startswith(prefix)]
    if len(matches) != 1:
        raise KeyError(f"Expected one paragraph beginning {prefix!r}; found {len(matches)}")
    return matches[0]


def body_index(body: etree._Element, node: etree._Element) -> int:
    return list(body).index(node)


def set_page_break_before(p: etree._Element) -> None:
    ppr = p.find("w:pPr", namespaces=NS)
    if ppr is None:
        ppr = etree.Element(W + "pPr")
        p.insert(0, ppr)
    if ppr.find("w:pageBreakBefore", namespaces=NS) is None:
        etree.SubElement(ppr, W + "pageBreakBefore")


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def signed(value: str) -> str:
    return f"{float(value):.3f}"


def update_table(root: etree._Element, basin_csv: Path) -> dict[str, list[str]]:
    rows = read_csv(basin_csv)
    lookup = {(r["Basin_Name"], r["Event_Type"]): r for r in rows}
    reservoirs = {"CSL": "0.073", "PSE": "0.546", "SWRB": "0.057", "YHH": "0.435", "YZR": "0.143"}
    table = root.find(".//w:body/w:tbl", namespaces=NS)
    if table is None:
        raise RuntimeError("Main Table 1 was not found")
    table_rows = table.xpath("./w:tr", namespaces=NS)
    if len(table_rows) != 6:
        raise RuntimeError(f"Expected six Table 1 rows; found {len(table_rows)}")
    output: dict[str, list[str]] = {}
    for tr in table_rows[1:]:
        cells = tr.xpath("./w:tc", namespaces=NS)
        basin = p_text(cells[0]).strip()
        flood = lookup[(basin, "Flood")]
        drought = lookup[(basin, "Drought")]
        values = [
            basin,
            reservoirs[basin],
            f"{signed(flood['Mean_Delta_CC_mean'])} ± {float(flood['Mean_Delta_CC_std']):.3f}",
            f"{signed(flood['Mean_Delta_HA_mean'])} ± {float(flood['Mean_Delta_HA_std']):.3f}",
            f"{float(flood['Pooled_DIndex']):.3f}",
            f"{signed(drought['Mean_Delta_CC_mean'])} ± {float(drought['Mean_Delta_CC_std']):.3f}",
            f"{signed(drought['Mean_Delta_HA_mean'])} ± {float(drought['Mean_Delta_HA_std']):.3f}",
            f"{float(drought['Pooled_DIndex']):.3f}",
        ]
        if len(cells) != len(values):
            raise RuntimeError(f"Unexpected Table 1 width for {basin}: {len(cells)}")
        for cell, text in zip(cells, values):
            paragraphs = cell.xpath("./w:p", namespaces=NS)
            if not paragraphs:
                p = etree.SubElement(cell, W + "p")
            else:
                p = paragraphs[0]
                for extra in paragraphs[1:]:
                    cell.remove(extra)
            set_plain(p, text)
        output[basin] = values
    return output


def update_drawing_extents(root: etree._Element, ratios: list[float]) -> None:
    drawings = root.xpath(".//w:body/w:p[.//w:drawing]", namespaces=NS)
    if len(drawings) != 7:
        raise RuntimeError(f"Expected seven drawing paragraphs; found {len(drawings)}")
    for paragraph, ratio in zip(drawings[3:7], ratios):
        extent = paragraph.find(".//wp:extent", namespaces=NS)
        a_extent = paragraph.find(".//a:xfrm/a:ext", namespaces=NS)
        if extent is None or a_extent is None:
            raise RuntimeError("Drawing extent was not found")
        width = int(extent.get("cx"))
        height = round(width / ratio)
        extent.set("cy", str(height))
        a_extent.set("cy", str(height))


def revise_document(xml_bytes: bytes, basin_csv: Path, figure_ratios: list[float]) -> tuple[bytes, dict[str, object]]:
    parser = etree.XMLParser(remove_blank_text=False)
    root = etree.fromstring(xml_bytes, parser)
    body = root.find(".//w:body", namespaces=NS)
    if body is None:
        raise RuntimeError("document.xml has no body")
    before_begins = len(root.xpath('.//w:fldChar[@w:fldCharType="begin"]', namespaces=NS))
    before_ends = len(root.xpath('.//w:fldChar[@w:fldCharType="end"]', namespaces=NS))
    before_cites = len(re.findall(rb"ADDIN\s+EN\.CITE", xml_bytes))
    before_reflist = len(re.findall(rb"ADDIN\s+EN\.REFLIST", xml_bytes))
    refs = find_exact(body, "References")
    refs_before = [etree.tostring(n, method="c14n") for n in list(body)[body_index(body, refs):]]

    replacements = [
        (
            "Impacts of Climate Change and Aggregate Human Activities on Flood and Drought Frequencies in China",
            NEW_TITLE,
        ),
        (
            "Climate forcing and aggregate human activities jointly shape runoff variability",
            "Climate forcing and aggregate human activities jointly shape runoff variability, complicating their separation in model-scenario analyses of hydrological extremes. We analysed daily runoff from seven ISIMIP3a global hydrological models over mainland China for 1950–2019. Within each model-grid, the factual obsclim-histsoc simulation supplied the monthly 30-day SRI calibration that was applied unchanged to the factual and two counterfactual scenarios. Flood and drought events were then detected separately in every model-scenario series. Frequency ratios compared 1950–1984 with 1985–2019, and paired scenario contrasts quantified climate (ΔCC), aggregate human activity (ΔHA), and total (ΔTotal) components before equal-weight multi-model aggregation. Scenario-specific SRI calibration was retained as a sensitivity analysis.",
        ),
        (
            "The recomputed maps show spatially heterogeneous flood and drought responses.",
            "Under the common factual SRI reference, basin-scale scenario contrasts were more stable than fine-scale spatial patterning. Flood pooled DIndex values were 0.634–0.864 across the five basin groups, indicating a larger absolute climate component in every basin. Four drought basin groups also had a larger climate component (pooled DIndex 0.645–0.923), whereas Yellow–Huai–Hai was near parity with a slightly larger aggregate human-activity magnitude (0.478). These classifications were unchanged by leave-one-model-out analysis and alternative period splits; only Yellow–Huai–Hai flood crossed 0.5 across the nine event definitions. Basin dominance was also unchanged under scenario-specific calibration, although flood grid-scale correspondence between calibration schemes was weak. The defensible conclusion is therefore a robust basin-scale magnitude contrast with a method-sensitive near-parity case, not a universal or fine-scale causal mechanism.",
        ),
        (
            "To compare flood and drought attribution across China within a consistent spatial design",
            "To compare flood and drought attribution across China within a consistent spatial design, the analysis used the common 0.5° × 0.5° ISIMIP3a grid. Coordinate alignment, national-boundary masking, and quality control produced 3,823 mainland-China candidate grid centres in every model-scenario output. Model-specific reference-fit exclusions were retained as quality-control rows, so the number of valid attribution cells varied among models from 2,103 to 3,823. Results are interpreted using five basin groupings: YZR, YHH, CSL, PSE, and SWRB.",
        ),
        (
            "Within each model–scenario–grid series, daily runoff was converted",
            "Within each model-grid, daily runoff was converted to a 30-day backward-looking mean. The factual obsclim-histsoc series supplied the common 1950–2019 monthly calibration for all three scenarios. For each calendar month, positive factual values were divided by their median for numerical fitting; lognormal, gamma, Gumbel, and Weibull candidates were fitted, and the finite fit with the smallest Akaike information criterion was selected. The fitted family, parameters, zero-flow probability, and scale were then applied unchanged to obsclim-histsoc, counterclim-histsoc, and counterclim-1901soc. This common reference preserves scenario differences on one runoff-anomaly scale. A secondary workflow fitted each scenario separately to quantify calibration sensitivity.",
        ),
        (
            "where Dᵢ,ⱼ denotes the fitted month-specific mixed cumulative distribution",
            "where Dᶠᵃᶜᵗᵢ,ⱼ denotes the factual month-specific mixed cumulative distribution for the 30-day runoff series at grid cell (i,j), including the zero-runoff probability mass, and Φ⁻¹ is the inverse standard-normal cumulative distribution function. Parameters were fitted over 1950–2019 from obsclim-histsoc and applied to all three scenarios. The primary transformation therefore defines counterfactual extremes relative to the corresponding factual model-grid-month reference. Figure 2 is a conceptual illustration of this mapping.",
        ),
        (
            "All model-scenario combinations were processed with the same SRI, event, and frequency-ratio settings.",
            "All model-scenario combinations were processed with the same factual-reference transformation, event rules, and frequency-ratio settings. Events were identified within each model-scenario series before ensemble aggregation; daily SRI was never averaged across models. The three scenarios were paired within each model before ΔTotal, ΔHA, and ΔCC were calculated.",
        ),
        (
            "The gridded outputs report the available-model mean",
            "The gridded outputs report the available-model mean, standard deviation, interquartile range, valid-model count, and proportion of models agreeing with the ensemble-mean sign. Of 3,823 mainland grid cells, 1,743 have all seven paired models; 18, 90, 685, and 1,287 cells have three, four, five, and six paired models, respectively. The maps therefore show available-model means rather than a fixed seven-model mean at every location. Black stippling identifies cells where at least five available models agree with the ensemble-mean sign.",
        ),
        (
            "Robustness checks were deliberately limited.",
            "Robustness checks targeted the choices most likely to affect the attribution. Each hazard used nine event definitions: drought thresholds of -0.5, -0.8, and -1.0 crossed with minimum durations of 10, 20, and 30 days, and flood thresholds of 0.5, 0.8, and 1.0 crossed with reset gaps of 10, 20, and 30 days. Additional checks used split years 1980, 1985, and 1990, an alternative N1 = 0 undefined rule, leave-one-model-out basin aggregation, and scenario-specific rather than common-reference SRI calibration.",
        ),
        (
            "Figure 4. Recomputed model-wise workflow",
            "Figure 4. Reference-calibrated model-wise workflow for extreme-runoff frequency scenario contrasts. Within each model-grid, the factual obsclim-histsoc series supplies the monthly 30-day SRI calibration applied to all three scenarios. Process events and corrected frequency ratios are calculated separately within each model-scenario series before the scenarios are paired to derive ΔCC, ΔHA, and ΔTotal; daily SRI is not averaged across models. Available-model summaries retain spread, model counts, and sign agreement. Robustness checks vary event definitions, period splits, calibration scheme, the zero-event rule, and omitted model.",
        ),
        (
            "The recomputed drought ΔTotal field is spatially heterogeneous.",
            "The common-reference drought maps contain positive and negative grid-cell contrasts, but their fine-scale structure is treated as descriptive because calibration choice materially changes local component values. Basin summaries provide the primary inference: four basin groups have pooled drought DIndex values of 0.645–0.923, whereas YHH is near parity at 0.478.",
        ),
        (
            "Figure 5. Available-model mean total scenario contrast",
            "Figure 5. Available-model mean total scenario contrast (ΔTotal) in process-event frequency ratios under the common factual SRI reference. ΔTotal is calculated within each model as obsclim-histsoc minus counterclim-1901soc before equal-weight aggregation. (a) Flood events. (b) Drought events. Cells with fewer than three paired models are not shown. Black stippling indicates that at least five models agree with the sign of the ensemble mean. Maps are descriptive grid-scale summaries; calibration sensitivity is reported in the Supplementary Information.",
        ),
        (
            "Figure 6. Available-model mean climate scenario contrast",
            "Figure 6. Available-model mean climate scenario contrast (ΔCC) in process-event frequency ratios under the common factual SRI reference. ΔCC is calculated within each model as obsclim-histsoc minus counterclim-histsoc before equal-weight aggregation. (a) Flood events. (b) Drought events. Cells with fewer than three paired models are not shown. Black stippling indicates that at least five models agree with the ensemble-mean sign. Fine-scale interpretation is limited by calibration sensitivity.",
        ),
        (
            "The recomputed attribution maps separate the climate scenario contrast",
            "Figures 6 and 7 separate the climate and aggregate human-activity scenario contrasts, but neither map identifies an isolated local mechanism. The common-reference and scenario-specific workflows retained the same basin dominance classification in all ten basin-event comparisons, while drought grid-scale agreement was moderate and flood grid-scale agreement was weak.",
        ),
        (
            "The drought maps differ from the flood maps mainly in spatial coherence.",
            "At basin scale, CSL, PSE, SWRB, and YZR retained larger absolute climate components for drought. YHH differed: its pooled DIndex was 0.478, with two of seven models climate-larger and five aggregate-human-activity-larger. This is evidence of a method-sensitive near-parity case, not nationwide human dominance of drought.",
        ),
        (
            "Figures 6 and 7 show that the spatially heterogeneous drought ΔTotal field",
            "The signed YHH drought means were ΔCC = -0.149 ± 0.459 and ΔHA = 0.232 ± 0.381 across models. Their large spreads relative to the means reinforce that DIndex and signed direction answer different questions. The pooled magnitude result does not identify irrigation, groundwater abstraction, land-use change, or reservoir regulation as an individual cause.",
        ),
        (
            "Drought events depend on cumulative water-balance deficits and continuity.",
            "Drought results remained classification-stable across nine threshold-duration definitions, three period splits, and seven leave-one-model-out runs. For YHH, pooled DIndex ranged from 0.459 to 0.478 across event definitions, 0.453 to 0.496 across period splits, and 0.467 to 0.486 when individual models were omitted.",
        ),
        (
            "The YHH interpretation requires particular caution.",
            "The YHH interpretation therefore requires particular caution. Its pooled drought DIndex of 0.478 indicates a slightly larger aggregate human-activity magnitude, but values remain close to 0.5 and between-model spread is substantial. We describe this as near parity rather than decisive human dominance.",
        ),
        (
            "The recomputed drought field also contains regions",
            "Calibration sensitivity further limits local inference. Between common-reference and scenario-specific calibration, drought ΔTotal had r = 0.882 and sign concordance = 0.802 across 3,823 cells, whereas drought ΔHA had r = 0.392 and sign concordance = 0.651. Basin classifications were more stable than individual grid-cell components.",
        ),
        (
            "The results do not support a simple claim that floods are climate driven",
            "The results do not support a simple claim that floods are climate driven while droughts are human driven. The evidence supports larger basin-scale climate magnitudes for both hazards in four basin groups, larger climate magnitudes for YHH flood under the primary definition, and near parity for YHH drought.",
        ),
        (
            "Because the main drought maps use process-based event-count ratios",
            "Because the maps use process-based event counts, the drought result concerns the frequency of sustained standardized runoff-deficit episodes rather than threshold-day totals. Event-definition sensitivity bounds this dependence but does not make one threshold-duration combination uniquely correct.",
        ),
        (
            "The recomputed flood ΔTotal field is predominantly negative",
            "The common-reference flood maps show localized positive and negative contrasts, but fine-scale patterns were not robust to calibration choice. The primary flood inference therefore comes from basin-level absolute magnitudes and their robustness checks rather than from a particular national grid pattern.",
        ),
        (
            "Figure 7. Available-model mean aggregate human-activity scenario contrast",
            "Figure 7. Available-model mean aggregate human-activity scenario contrast (ΔHA) in process-event frequency ratios under the common factual SRI reference. ΔHA is calculated within each model as counterclim-histsoc minus counterclim-1901soc before equal-weight aggregation. (a) Flood events. (b) Drought events. Cells with fewer than three paired models are not shown. Black stippling indicates that at least five models agree with the ensemble-mean sign. ΔHA combines model-represented socio-economic differences and is not attribution to any single human driver.",
        ),
        (
            "Across the five basin groups, the recomputed flood pooled DIndex ranges",
            "Across the five basin groups, common-reference flood pooled DIndex ranged from 0.634 in PSE to 0.864 in SWRB. The absolute climate scenario contrast was therefore larger in every primary basin summary, although PSE and YHH were closer to parity and several signed component means were small relative to between-model standard deviations.",
        ),
        (
            "Figure 5 shows the sign and spatial distribution of the total flood contrast.",
            "Figures 5–7 provide descriptive common-reference grid summaries. Their local pattern should not be interpreted as a robust mechanism: between calibration schemes, flood grid-scale r was -0.172 for ΔTotal and -0.225 for ΔCC, with sign concordance of 0.599 and 0.582, respectively.",
        ),
        (
            "Within the paired scenario framework, the climate contrast accounts",
            "Within the paired framework, the climate component was larger in seven of seven models for CSL, SWRB, and YZR flood, six of seven for PSE, and five of seven for YHH. Leave-one-model-out pooled DIndex remained above 0.5 for every flood basin, but YHH flood crossed 0.5 across the nine event definitions.",
        ),
        (
            "Flood responses are more fragmented in eastern monsoon-influenced regions",
            "The flood evidence therefore supports a basin-scale relative-magnitude result rather than a deterministic local causal interpretation. The analysis does not include precipitation-event diagnostics or local reservoir operations, so grid-cell contrasts cannot be assigned to an individual meteorological or management process.",
        ),
        (
            "The larger climate-attribution component may reflect the short response time of floods.",
            "A faster runoff response offers a plausible context for the larger flood climate component, while routing, reservoirs, land cover, and withdrawals can modify local high-flow sequences. These processes were not separately tested here and are not used as proof of the basin result.",
        ),
        (
            "Aggregate human activity can still matter locally.",
            "Aggregate human activity can still matter locally, but the scenario design combines multiple land- and water-management differences within ΔHA. The primary basin summaries do not support a larger aggregate human-activity flood component, and the calibration comparison prevents stronger claims about the location of individual effects.",
        ),
        (
            "The recomputed basin statistics show a larger absolute climate component",
            "The common-reference basin statistics show a larger absolute climate component for floods in all five groups, with pooled DIndex values of 0.634–0.864. Drought pooled DIndex was 0.656 in CSL, 0.645 in PSE, 0.923 in SWRB, 0.478 in YHH, and 0.713 in YZR. The first, second, third, and fifth values indicate a larger climate magnitude; YHH remains near parity with a slightly larger aggregate human-activity magnitude.",
        ),
        (
            "Table 1. Recomputed basin-scale scenario contrasts",
            "Table 1. Common-reference basin-scale scenario contrasts for changes in process-event frequency ratios. Signed ΔCC and ΔHA values are seven-model means ± between-model standard deviations. Reservoir intensity is storage capacity normalized by basin area (10⁶ m³ km⁻²) and is retained only as a diagnostic descriptor. Pooled DIndex is calculated from model-mean absolute component magnitudes; it is not a percentage contribution.",
        ),
        (
            "Reservoir intensity alone does not track the recomputed ΔHA pattern.",
            "Reservoir intensity alone did not track the signed ΔHA pattern. PSE had the highest intensity (0.546) but small signed ΔHA means (-0.002 ± 0.018 for flood; -0.003 ± 0.025 for drought). YHH combined high intensity (0.435) with a positive drought ΔHA mean (0.232 ± 0.381), while CSL had low intensity (0.073) and a drought ΔHA mean of 0.216 ± 0.447. These five cases are descriptive and do not support a reservoir-specific causal inference.",
        ),
        (
            "CSL demonstrates why signed means",
            "CSL demonstrates why signed means, absolute magnitudes, and model spread must be reported together. Its drought ΔHA mean was positive, but its standard deviation was more than twice the mean; its pooled DIndex nevertheless remained above 0.5 in every robustness family.",
        ),
        (
            "PSE illustrates the opposite diagnostic case.",
            "PSE had the highest reservoir intensity but small signed attribution means. Its pooled DIndex remained climate-larger under calibration, event-definition, period, and leave-one-model-out checks, showing that storage capacity normalized by basin area is insufficient to predict ΔHA.",
        ),
        (
            "SWRB has the largest pooled drought DIndex",
            "SWRB had the largest pooled drought DIndex (0.923), and YZR retained larger climate magnitudes for both hazards. These basin-scale results are robust magnitude comparisons; they do not establish uniform mechanisms within either basin group.",
        ),
        (
            "YHH remains the most sensitive interpretive case.",
            "YHH remained the most sensitive interpretive case. Its drought pooled DIndex was 0.478 and remained below but close to 0.5 across the tested common-reference checks. Its flood pooled DIndex was 0.547 and crossed 0.5 across alternative event definitions, so both results warrant near-parity language.",
        ),
        (
            "Under the primary drought definition",
            "Across the nine event definitions, pooled DIndex classifications were stable for all basin-event pairs except YHH flood, which ranged from 0.459 to 0.555. Period-split classifications were stable for all pairs. YHH drought remained closest to parity, ranging from 0.453 to 0.496 across the 1980, 1985, and 1990 splits.",
        ),
        (
            "The sensitivity analysis should therefore be read",
            "Calibration sensitivity separated robust basin inference from fragile local patterning. All ten basin dominance classifications were unchanged between common-reference and scenario-specific SRI fitting, with a maximum pooled DIndex change of 0.074. In contrast, grid-scale flood components showed weak correspondence between schemes; the main maps are therefore descriptive rather than evidence of a stable fine-scale mechanism.",
        ),
        (
            "The sensitivity analyses do not remove uncertainty",
            "Leave-one-model-out analysis preserved the primary side of 0.5 for every basin-event pair, with YHH drought remaining at 0.467–0.486. The alternative zero-event rule changed some signed component values but did not overturn the broad basin magnitude pattern. These checks support qualified robustness, not uniqueness of the selected rules.",
        ),
        (
            "Taken together, the recomputed maps and basin statistics",
            "Taken together, the evidence distinguishes robust scale from descriptive detail. Basin-level climate-versus-aggregate-human magnitude classifications are stable across calibration, period, and model omission, while the YHH near-parity results and fine-scale flood maps remain sensitive to methodological choice.",
        ),
        (
            "Second, model availability varies spatially",
            "Second, model availability varies spatially because failed factual monthly reference fits or internal SRI gaps exclude individual model-grids. Of 3,823 candidate cells, 1,743 contain all seven paired models; 18, 90, 685, and 1,287 contain three, four, five, and six models. Equal weighting avoids substituting missing outputs but changes ensemble composition across space, so local map features require caution.",
        ),
        (
            "Event-definition uncertainty is another important boundary.",
            "Event construction is another boundary. Both hazards were tested across nine threshold-duration or threshold-reset combinations, but these definitions remain methodological choices. YHH flood crossed the 0.5 DIndex boundary across alternatives, and the common-reference versus scenario-specific comparison showed that flood grid patterns also depend strongly on calibration. Sensitivity analysis therefore narrows defensible claims without making one definition uniquely correct.",
        ),
        (
            "Model limitations are especially relevant in heavily managed basins.",
            "Model limitations are especially relevant in heavily managed basins. Global models simplify reservoir operation, irrigation schedules, groundwater pumping, transfers, and emergency decisions. YHH drought is consequently interpreted as a near-parity aggregate scenario result (pooled DIndex 0.478), not evidence that a specific management process caused the frequency change.",
        ),
        (
            "This study compared changes in flood and drought event frequencies",
            "This study compared changes in flood and drought event frequencies across mainland China during 1950–2019 using seven ISIMIP3a global hydrological models. The factual simulation supplied a common monthly 30-day SRI calibration for all three scenarios, events were detected separately within each model-scenario series, and paired frequency-ratio contrasts quantified climate, aggregate human-activity, and total components before ensemble aggregation. Scenario-specific calibration, alternative event definitions and period splits, the zero-event rule, and leave-one-model-out aggregation were evaluated as robustness checks.",
        ),
        (
            "Flood-frequency contrasts have a larger absolute climate component",
            "Flood-frequency contrasts had a larger absolute climate component in all five primary basin summaries (pooled DIndex 0.634–0.864). Four drought basin groups also retained larger climate components (0.645–0.923), whereas YHH drought was near parity with a slightly larger aggregate human-activity magnitude (0.478). Basin classifications were stable across calibration schemes, period splits, and omitted models; YHH flood crossed 0.5 across event definitions. These results support basin-scale relative-magnitude inference, not nationwide human dominance of drought or universal causal regimes.",
        ),
        (
            "These contrasting regimes favor basin-specific risk interpretation.",
            "The practical implication is to retain basin and hazard distinctions while respecting the model-scenario evidence scale. Climate-informed planning remains relevant for both hazards in most basin groups, whereas YHH requires joint consideration of climatic and aggregate land-water influences. Because fine-scale flood patterns were calibration-sensitive, the strongest result is the basin-level magnitude contrast and the identification of YHH drought as a near-parity case requiring localized process data.",
        ),
    ]

    for prefix, text in replacements:
        set_plain(find_start(body, prefix), text)

    set_plain(find_exact(body, "2.3 Counterfactual attribution framework"), "2.3 Counterfactual scenario-contrast framework")
    set_plain(find_exact(body, "3.1 Drought-frequency changes and attribution"), "3.1 Drought-frequency changes and scenario contrasts")
    set_plain(find_exact(body, "3.2 Flood-frequency changes and attribution"), "3.2 Flood-frequency changes and scenario contrasts")
    replace_text_nodes(root, "attribution was conducted using frequency-ratio contrasts", "the components were quantified using frequency-ratio contrasts")
    replace_text_nodes(root, "stepwise counterfactual attribution decomposition", "stepwise counterfactual scenario-contrast decomposition")
    replace_text_nodes(root, "The attribution logic relies", "The scenario-contrast logic relies")
    replace_text_nodes(root, "first-order attribution approximation", "first-order scenario-contrast approximation")
    replace_text_nodes(
        root,
        "This pattern accords with the rapid response of flood extremes to precipitation variability and runoff routing.",
        "The larger basin-scale climate component for flood-event frequency is consistent with, but does not demonstrate, sensitivity of high-runoff generation and routing to precipitation variability.",
    )
    replace_text_nodes(root, "complement rather than replace scenario-based attribution", "complement rather than replace scenario-contrast analysis")
    replace_text_nodes(root, "this is a diagnostic comparison rather than a formal correlation analysis.", "this is a diagnostic comparison rather than a formal statistical association test.")
    body.remove(find_exact(body, "The results do not support a simple claim that floods are climate driven while droughts are human driven. The evidence supports larger basin-scale climate magnitudes for both hazards in four basin groups, larger climate magnitudes for YHH flood under the primary definition, and near parity for YHH drought."))
    body.remove(find_exact(body, "Table 1 supports a bounded contrast between hazards. The flood absolute climate component is larger in every basin group, whereas drought partitioning varies more strongly and includes YHH near parity. The basin evidence does not support nationwide human-activity dominance of drought or two universally distinct physical mechanisms."))

    set_page_break_before(find_start(body, "Table 1. Common-reference basin-scale scenario contrasts"))

    table_values = update_table(root, basin_csv)
    update_drawing_extents(root, figure_ratios)

    refs = find_exact(body, "References")
    conclusion_heading = find_exact(body, "4. Conclusions")
    conclusion_template = find_start(body, "This study compared changes in flood and drought event frequencies")
    insertion = body_index(body, refs)
    availability_nodes = [
        clone_plain(conclusion_heading, "Data Availability"),
        clone_plain(conclusion_template, "The ISIMIP3a input simulations used in this study are available through the ISIMIP data infrastructure under their respective access and citation terms. The processed model-event tables, validation outputs, basin and grid summaries, and figure source data supporting this manuscript will be deposited in a public repository. The repository name and persistent identifier have not yet been assigned; until deposition, these derived files are retained in the authors’ reproducibility archive."),
        clone_plain(conclusion_heading, "Code Availability"),
        clone_plain(conclusion_template, "The analysis scripts for factual-reference SRI calibration and transformation, event detection, paired attribution, sensitivity analysis, validation, aggregation, and figure generation will be deposited in a public repository. The repository name and persistent identifier have not yet been assigned; the scripts are currently retained in the authors' reproducibility archive."),
    ]
    for offset, node in enumerate(availability_nodes):
        body.insert(insertion + offset, node)

    after_begins = len(root.xpath('.//w:fldChar[@w:fldCharType="begin"]', namespaces=NS))
    after_ends = len(root.xpath('.//w:fldChar[@w:fldCharType="end"]', namespaces=NS))
    serialized = etree.tostring(root, xml_declaration=True, encoding="UTF-8", standalone="yes")
    after_cites = len(re.findall(rb"ADDIN\s+EN\.CITE", serialized))
    after_reflist = len(re.findall(rb"ADDIN\s+EN\.REFLIST", serialized))
    refs_after_node = find_exact(body, "References")
    refs_after = [etree.tostring(n, method="c14n") for n in list(body)[body_index(body, refs_after_node):]]
    if (before_begins, before_ends, before_cites, before_reflist) != (after_begins, after_ends, after_cites, after_reflist):
        raise RuntimeError(
            "EndNote field regression: "
            f"{(before_begins, before_ends, before_cites, before_reflist)} -> "
            f"{(after_begins, after_ends, after_cites, after_reflist)}"
        )
    if refs_before != refs_after:
        raise RuntimeError("References subtree changed")

    stats = {
        "paragraph_replacements": len(replacements),
        "availability_paragraphs_added": len(availability_nodes),
        "field_begins_before_after": [before_begins, after_begins],
        "field_ends_before_after": [before_ends, after_ends],
        "endnote_cites_before_after": [before_cites, after_cites],
        "endnote_reflist_before_after": [before_reflist, after_reflist],
        "references_subtree_unchanged": True,
        "table_values": table_values,
    }
    return serialized, stats


def build(source: Path, output: Path, figure_dir: Path, basin_csv: Path, report: Path | None = None) -> dict[str, object]:
    if source.resolve() == output.resolve():
        raise ValueError("Refusing to overwrite the source manuscript")
    figure_names = [
        "Figure4_CommonReferenceWorkflow.png",
        "Figure5_DeltaTotal.png",
        "Figure6_DeltaCC.png",
        "Figure7_DeltaHA.png",
    ]
    figure_paths = [figure_dir / name for name in figure_names]
    missing = [str(path) for path in [source, basin_csv, *figure_paths] if not path.exists()]
    if missing:
        raise FileNotFoundError("Missing required input: " + "; ".join(missing))
    ratios = []
    for path in figure_paths:
        with Image.open(path) as image:
            ratios.append(image.width / image.height)

    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="q1_manuscript_") as td:
        root_dir = Path(td)
        with zipfile.ZipFile(source) as zin:
            zin.extractall(root_dir)
        xml_path = root_dir / "word" / "document.xml"
        revised, stats = revise_document(xml_path.read_bytes(), basin_csv, ratios)
        xml_path.write_bytes(revised)
        core_path = root_dir / "docProps" / "core.xml"
        core = etree.fromstring(core_path.read_bytes())
        title_nodes = core.xpath(".//dc:title", namespaces=NS)
        if title_nodes:
            title_nodes[0].text = NEW_TITLE
            core_path.write_bytes(etree.tostring(core, xml_declaration=True, encoding="UTF-8", standalone="yes"))
        for index, figure in enumerate(figure_paths, start=4):
            (root_dir / "word" / "media" / f"image{index}.png").write_bytes(figure.read_bytes())
        with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as zout:
            for path in sorted(root_dir.rglob("*")):
                if path.is_file():
                    zout.write(path, path.relative_to(root_dir).as_posix())
    stats["output"] = str(output.resolve())
    stats["figures_replaced"] = figure_names
    if report is not None:
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text(json.dumps(stats, indent=2, ensure_ascii=False), encoding="utf-8")
    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--figure-dir", type=Path, required=True)
    parser.add_argument("--basin-csv", type=Path, required=True)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    result = build(args.source, args.output, args.figure_dir, args.basin_csv, args.report)
    print(json.dumps(result, indent=2, ensure_ascii=False))
