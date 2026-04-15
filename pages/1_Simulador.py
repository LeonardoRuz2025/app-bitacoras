import streamlit as st
import streamlit.components.v1 as components

# Configuración de la página
st.set_page_config(page_title="Simulador de Telemetría", layout="wide")

# Tu código HTML, CSS y JS exacto
html_codigo = """
<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Simulador de Telemetría: Pulsos vs Límite DGA</title>
  <style>
    :root {
      --bg: #0b1020;
      --panel: #121a31;
      --panel-2: #182243;
      --text: #eaf0ff;
      --muted: #aeb8d8;
      --line: #2a3869;
      --ok: #2fbf71;
      --warn: #ff6b6b;
      --real: #4da3ff;
      --reported: #38d39f;
      --legal: #ff4d6d;
      --accent: #ffd166;
    }

    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Inter, Segoe UI, Roboto, Arial, sans-serif;
      background: linear-gradient(180deg, #09101f 0%, #0e1630 100%);
      color: var(--text);
      line-height: 1.55;
    }

    .wrap {
      max-width: 1400px;
      margin: 0 auto;
      padding: 28px;
    }

    .hero {
      background: linear-gradient(135deg, rgba(77,163,255,.14), rgba(255,209,102,.08));
      border: 1px solid var(--line);
      border-radius: 22px;
      padding: 28px;
      box-shadow: 0 18px 50px rgba(0,0,0,.28);
    }

    h1, h2, h3 { margin: 0 0 12px; }
    h1 { font-size: 2rem; }
    h2 { font-size: 1.45rem; margin-top: 28px; }
    h3 { font-size: 1.08rem; margin-top: 18px; }
    p, li { color: var(--muted); }
    strong { color: var(--text); }

    .badge-row {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 16px;
    }

    .badge {
      background: rgba(255,255,255,.04);
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 8px 12px;
      font-size: .92rem;
      color: var(--text);
    }

    .grid {
      display: grid;
      gap: 20px;
      margin-top: 22px;
    }

    .grid-2 {
      grid-template-columns: 1.2fr .8fr;
    }

    .panel {
      background: rgba(18,26,49,.88);
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 22px;
      box-shadow: 0 10px 30px rgba(0,0,0,.18);
    }

    .controls {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 16px;
      align-items: end;
    }

    .control {
      background: rgba(255,255,255,.03);
      border: 1px solid rgba(255,255,255,.08);
      border-radius: 16px;
      padding: 14px;
    }

    label {
      display: block;
      font-weight: 700;
      margin-bottom: 10px;
    }

    input[type="range"] { width: 100%; }
    input[type="number"], select {
      width: 100%;
      background: #0c1430;
      color: var(--text);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 10px 12px;
      font-size: 0.95rem;
    }

    .range-input-row {
      display: grid;
      grid-template-columns: minmax(0, 1fr) 110px;
      gap: 10px;
      align-items: center;
    }

    .mini {
      font-size: .86rem;
      color: var(--muted);
      margin-top: 8px;
    }

    .value {
      font-size: 1.3rem;
      font-weight: 800;
      color: var(--accent);
      margin-top: 8px;
    }

    .chart-wrap {
      width: 100%;
      overflow: hidden;
      border-radius: 18px;
      border: 1px solid rgba(255,255,255,.08);
      background: linear-gradient(180deg, rgba(255,255,255,.02), rgba(255,255,255,.01));
      padding: 14px;
    }

    svg { width: 100%; height: auto; display: block; }
    .legend {
      display: flex;
      flex-wrap: wrap;
      gap: 14px;
      margin-top: 12px;
      color: var(--muted);
      font-size: 0.92rem;
    }

    .legend-item { display: inline-flex; align-items: center; gap: 8px; }
    .swatch {
      width: 24px;
      height: 4px;
      border-radius: 999px;
      display: inline-block;
    }

    .cards {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 14px;
      margin-top: 16px;
    }

    .metric {
      padding: 18px;
      border-radius: 18px;
      background: var(--panel-2);
      border: 1px solid var(--line);
    }

    .metric .k {
      color: var(--muted);
      font-size: .88rem;
      margin-bottom: 8px;
    }

    .metric .v {
      font-size: 1.55rem;
      font-weight: 800;
    }

    .alert {
      margin-top: 16px;
      padding: 16px 18px;
      border-radius: 18px;
      border: 1px solid var(--line);
      font-weight: 700;
    }

    .alert.ok { background: rgba(47,191,113,.12); color: #9ff0bf; border-color: rgba(47,191,113,.35); }
    .alert.warn { background: rgba(255,107,107,.12); color: #ffb3b3; border-color: rgba(255,107,107,.35); }

    table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 16px;
      font-size: .94rem;
      overflow: hidden;
      border-radius: 16px;
    }

    th, td {
      border-bottom: 1px solid rgba(255,255,255,.08);
      padding: 10px 12px;
      text-align: left;
      vertical-align: top;
    }

    th {
      color: var(--text);
      background: rgba(255,255,255,.04);
      position: sticky;
      top: 0;
    }

    tbody tr:nth-child(odd) { background: rgba(255,255,255,.02); }

    .formula {
      background: rgba(255,255,255,.04);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 14px 16px;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      color: #dfe6ff;
      overflow: auto;
    }

    .note {
      border-left: 4px solid var(--accent);
      padding: 10px 14px;
      background: rgba(255,209,102,.08);
      border-radius: 10px;
      color: #f7e7b7;
      margin-top: 12px;
    }

    .smallchart {
      height: 220px;
    }

    @media (max-width: 1100px) {
      .grid-2, .controls, .cards { grid-template-columns: 1fr 1fr; }
    }

    @media (max-width: 760px) {
      .wrap { padding: 16px; }
      .grid-2, .controls, .cards { grid-template-columns: 1fr; }
      .range-input-row { grid-template-columns: 1fr; }
      h1 { font-size: 1.6rem; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <h1>Simulador técnico de telemetría por pulsos y cumplimiento DGA</h1>
      <p>
        Este archivo explica con precisión cómo un sistema de telemetría basado en <strong>pulsos discretos</strong>
        puede mostrar <strong>picos aparentes</strong> en la plataforma, aun cuando el pozo esté operando dentro del
        derecho de aprovechamiento. El modelo considera un flujómetro con resolución de <strong>1 pulso = 1000 litros = 1 m³</strong>
        y una consolidación de datos cada <strong>1 hora exacta</strong>.
      </p>
      <div class="badge-row">
        <span class="badge">Proyecto: Monitoreo de Pozos</span>
        <span class="badge">Empresa: Unisource Ingeniería</span>
        <span class="badge">Modo de reporte: 1 hora</span>
        <span class="badge">Resolución: 1 m³ por pulso</span>
      </div>
    </section>

    <div class="grid grid-2">
      <section class="panel">
        <h2>1. Contexto normativo y operacional</h2>
        <p>
          Para cada pozo, la Dirección General de Aguas fija un <strong>Derecho de Aprovechamiento de Aguas (DAA)</strong>.
          En términos prácticos, existen dos restricciones críticas:
        </p>
        <ol>
          <li><strong>Caudal máximo instantáneo permitido (L/s).</strong></li>
          <li><strong>Volumen máximo acumulado en un período regulatorio</strong> (por ejemplo, diario, mensual o anual).</li>
        </ol>
        <p>
          El sistema de telemetría no “ve” el flujo como una señal continua perfecta. En realidad, detecta paquetes discretos de agua.
          Cada vez que el flujómetro completa <strong>1000 litros</strong>, emite un pulso. El datalogger cuenta los pulsos recibidos dentro de la hora,
          y desde eso calcula el caudal que luego aparece en plataforma.
        </p>

        <h3>Fórmula de caudal reportado</h3>
        <div class="formula">Caudal reportado (L/s) = (Pulsos en la hora × 1000 L) / 3600 s = Pulsos en la hora / 3.6</div>

        <div class="note">
          Una consecuencia fundamental de esta fórmula es que el caudal horario reportado solo puede tomar saltos discretos:
          0.00 L/s, 0.28 L/s, 0.56 L/s, 0.83 L/s, 1.11 L/s, etc. No existe una resolución más fina mientras el flujómetro siga entregando 1 m³ por pulso.
        </div>
      </section>

      <section class="panel">
        <h2>2. Lectura rápida del fenómeno</h2>
        <div class="cards" style="grid-template-columns: 1fr 1fr;">
          <div class="metric">
            <div class="k">Pozos de alto flujo</div>
            <div class="v">60–220 L/s</div>
            <p>La cantidad de pulsos por hora es tan alta que el error de cuantización se vuelve casi invisible.</p>
          </div>
          <div class="metric">
            <div class="k">Pozos de bajo flujo</div>
            <div class="v">&lt; 2 L/s</div>
            <p>La discretización domina el comportamiento y aparecen dientes de sierra y picos aparentes.</p>
          </div>
        </div>

        <h3>Ejemplos clave</h3>
        <ul>
          <li><strong>150 L/s:</strong> en una hora se extraen 540 m³, por lo que llegan 540 pulsos. La plataforma dibuja una línea prácticamente perfecta.</li>
          <li><strong>0.20 L/s:</strong> en una hora se extraen 0.72 m³. En muchas horas no llega ni un pulso completo. La plataforma alterna entre 0 y 0.28 L/s.</li>
        </ul>

        <h3>Idea central para auditoría</h3>
        <p>
          Un dato horario aislado <strong>no siempre representa el caudal real instantáneo</strong>. En caudales bajos,
          el valor horario visible es el resultado de <strong>cómo se repartieron los pulsos</strong> dentro de la ventana de una hora.
          Por eso, el análisis correcto siempre debe contrastar <strong>la gráfica</strong> con <strong>el totalizador acumulado</strong>.
        </p>
      </section>
    </div>

    <section class="panel" id="simulador">
      <h2>3. Simulador interactivo</h2>
      <p>
        Ajusta el <strong>caudal real del pozo</strong>, el <strong>límite legal DGA</strong>, la <strong>duración del análisis</strong> y el <strong>tamaño del pulso del flujómetro</strong>.
        La visualización compara la realidad física con lo que el sistema reporta por pulsos discretos.
      </p>

      <div class="controls">
        <div class="control">
          <label for="realFlow">Caudal real del pozo (L/s)</label>
          <div class="range-input-row">
            <input id="realFlow" type="range" min="0.1" max="220" step="0.01" value="0.45" />
            <input id="realFlowInput" type="number" min="0.1" max="220" step="0.01" value="0.45" />
          </div>
          <div class="value" id="realFlowValue">0.45 L/s</div>
          <div class="mini">Flujo físico continuo de la bomba. Puedes mover la barra o escribir el valor exacto.</div>
        </div>

        <div class="control">
          <label for="legalFlow">Límite legal DGA (L/s)</label>
          <div class="range-input-row">
            <input id="legalFlow" type="range" min="0.1" max="220" step="0.01" value="0.50" />
            <input id="legalFlowInput" type="number" min="0.1" max="220" step="0.01" value="0.50" />
          </div>
          <div class="value" id="legalFlowValue">0.50 L/s</div>
          <div class="mini">Máximo permitido por el derecho de aprovechamiento. También editable por teclado.</div>
        </div>

        <div class="control">
          <label for="hours">Horas simuladas</label>
          <input id="hours" type="range" min="6" max="72" step="1" value="24" />
          <div class="value" id="hoursValue">24 horas</div>
          <div class="mini">Horizonte temporal del análisis.</div>
        </div>

        <div class="control">
          <label for="pulseVolume">Tamaño de pulso (litros/pulso)</label>
          <select id="pulseVolume">
            <option value="1000" selected>1000 L/pulso (caso principal)</option>
            <option value="100">100 L/pulso</option>
            <option value="10">10 L/pulso</option>
          </select>
          <div class="mini">Permite comparar cómo cambia la resolución del sistema.</div>
        </div>
      </div>

      <div class="cards">
        <div class="metric"><div class="k">Volumen real en el período</div><div class="v" id="realVolumePeriod">0.00 m³</div></div>
        <div class="metric"><div class="k">Volumen legal máximo en el período</div><div class="v" id="legalVolumePeriod">0.00 m³</div></div>
        <div class="metric"><div class="k">Volumen total reportado por pulsos</div><div class="v" id="reportedVolumePeriod">0.00 m³</div></div>
        <div class="metric"><div class="k">Horas con pico visible sobre límite</div><div class="v" id="hoursOverLimit">0</div></div>
      </div>

      <div id="statusBox" class="alert ok">CUMPLIMIENTO LEGAL OK.</div>

      <div class="chart-wrap" style="margin-top:16px;">
        <svg id="mainChart" viewBox="0 0 1100 480" aria-label="Gráfico principal"></svg>
      </div>
      <div class="legend">
        <span class="legend-item"><span class="swatch" style="background: var(--legal);"></span> Límite legal DGA</span>
        <span class="legend-item"><span class="swatch" style="background: var(--real);"></span> Caudal real del pozo</span>
        <span class="legend-item"><span class="swatch" style="background: var(--reported);"></span> Caudal reportado por pulsos</span>
      </div>

      <div class="grid grid-2" style="margin-top:18px;">
        <div>
          <h3>Cómo leer el gráfico principal</h3>
          <ul>
            <li>La <strong>línea azul</strong> representa el caudal real continuo de la bomba.</li>
            <li>La <strong>línea roja</strong> marca el umbral DGA.</li>
            <li>La <strong>línea verde</strong> representa lo que ve la plataforma al convertir pulsos a caudal horario.</li>
          </ul>
          <p>
            Si la línea verde cruza momentáneamente la roja, pero el volumen real total sigue bajo el máximo legal, estás frente a un
            <strong>falso positivo por discretización</strong>. Si la línea azul y el volumen total exceden el límite, se trata de
            <strong>sobreconsumo real</strong>.
          </p>
        </div>
        <div>
          <h3>Indicadores automáticos</h3>
          <ul>
            <li><strong>Sobreconsumo real:</strong> ocurre cuando el volumen real del período supera el volumen máximo legal.</li>
            <li><strong>Picos aparentes:</strong> horas donde el valor reportado supera el límite, aunque el volumen real acumulado no lo haga.</li>
            <li><strong>Error de cuantización:</strong> diferencia entre el volumen real continuo y el volumen reportado discreto.</li>
          </ul>
        </div>
      </div>

      <h3 style="margin-top:22px;">Acumulados en el tiempo</h3>
      <div class="chart-wrap">
        <svg id="accChart" viewBox="0 0 1100 320" class="smallchart" aria-label="Gráfico de acumulados"></svg>
      </div>
      <div class="legend">
        <span class="legend-item"><span class="swatch" style="background: var(--real);"></span> Volumen real acumulado</span>
        <span class="legend-item"><span class="swatch" style="background: var(--reported);"></span> Volumen acumulado reportado</span>
        <span class="legend-item"><span class="swatch" style="background: var(--legal);"></span> Volumen legal acumulado</span>
      </div>

      <h3 style="margin-top:22px;">Detalle hora a hora</h3>
      <div style="max-height: 420px; overflow:auto; border:1px solid rgba(255,255,255,.08); border-radius:16px;">
        <table>
          <thead>
            <tr>
              <th>Hora</th>
              <th>Volumen real de la hora (L)</th>
              <th>Pulsos de la hora</th>
              <th>Caudal reportado (L/s)</th>
              <th>Volumen real acumulado (m³)</th>
              <th>Volumen reportado acumulado (m³)</th>
              <th>Remanente interno (L)</th>
              <th>Diagnóstico</th>
            </tr>
          </thead>
          <tbody id="detailTable"></tbody>
        </table>
      </div>
    </section>

    <section class="panel">
      <h2>4. Interpretación técnica precisa</h2>

      <h3>4.1. Por qué aparece el “diente de sierra”</h3>
      <p>
        Cuando el caudal real es bajo, la extracción por hora puede ser menor que el tamaño de un pulso. En ese caso,
        el flujómetro acumula agua internamente hasta completar un pulso entero. Eso hace que algunas horas reporten
        <strong>0 pulsos</strong> y otras reporten <strong>1 o 2 pulsos</strong>, aunque la bomba haya trabajado estable.
      </p>

      <h3>4.2. Por qué puede haber una falsa alarma</h3>
      <p>
        Supón un límite legal de <strong>0.50 L/s</strong> y un caudal real constante de <strong>0.45 L/s</strong>. Físicamente,
        el pozo cumple. Sin embargo, como la plataforma solo recibe conteos enteros de pulsos, una hora puede concentrar 2 pulsos
        y dibujar un punto equivalente a <strong>0.56 L/s</strong>. Ese valor visible supera el límite, pero no implica automáticamente
        una infracción real. Lo correcto es verificar el <strong>volumen acumulado</strong>.
      </p>

      <h3>4.3. Cuándo sí existe un incumplimiento real</h3>
      <p>
        Si el caudal real de operación supera el límite legal durante un tiempo suficiente, entonces no solo habrá cruces visuales,
        sino que el <strong>volumen real acumulado</strong> y el <strong>volumen totalizado reportado</strong> tenderán a sobrepasar el máximo permitido.
        En caudales medios y altos, además, la señal reportada se parece mucho a la señal real, por lo que la interpretación es directa.
      </p>

      <h3>4.4. Regla práctica para terreno</h3>
      <ul>
        <li>En <strong>bajo flujo</strong>, interpreta la gráfica siempre junto con el totalizador.</li>
        <li>En <strong>alto flujo</strong>, una línea sostenida por sobre el límite suele corresponder a una condición real.</li>
        <li>Si aparece un pico aislado, revisa si el volumen legal del período fue realmente excedido.</li>
        <li>La mejor defensa técnica ante auditoría es demostrar el balance volumétrico acumulado.</li>
      </ul>
    </section>

    <section class="panel">
      <h2>5. Conclusión operativa</h2>
      <p>
        Este simulador muestra por qué un sistema basado en pulsos discretos puede generar lecturas horarias aparentemente contradictorias,
        sin que eso implique un mal funcionamiento del datalogger ni una infracción real. La interpretación correcta exige distinguir entre:
      </p>
      <ol>
        <li><strong>Caudal reportado horario:</strong> sensible a discretización y redondeos por ventana temporal.</li>
        <li><strong>Volumen acumulado total:</strong> evidencia más robusta para demostrar cumplimiento o sobreconsumo real.</li>
      </ol>
      <p>
        Para supervisión, auditoría o defensa técnica frente a cliente y DGA, la recomendación es simple:
        <strong>no evaluar un pozo de bajo flujo con un solo punto horario</strong>. Evaluar siempre el contexto completo,
        el patrón temporal y el totalizador acumulado.
      </p>
    </section>
  </div>

  <script>
    const $ = (id) => document.getElementById(id);

    const fmt = {
      ls: (v) => `${Number(v).toFixed(2)} L/s`,
      m3: (v) => `${Number(v).toFixed(2)} m³`,
      liters: (v) => `${Number(v).toFixed(0)} L`,
      num: (v) => Number(v).toFixed(2)
    };

    const state = {
      realFlow: 0.45,
      legalFlow: 0.50,
      hours: 24,
      pulseVolume: 1000
    };

    function clamp(value, min, max) {
      return Math.min(Math.max(value, min), max);
    }

    function syncNumericInputs() {
      $('realFlowInput').value = Number(state.realFlow).toFixed(2);
      $('legalFlowInput').value = Number(state.legalFlow).toFixed(2);
    }

    function getStepPath(points, x, y) {
      if (!points.length) return '';
      let d = `M ${x(points[0])} ${y(points[0])}`;
      for (let i = 1; i < points.length; i++) {
        d += ` L ${x(points[i])} ${y(points[i-1])}`;
        d += ` L ${x(points[i])} ${y(points[i])}`;
      }
      return d;
    }

    function getLinePath(points, x, y) {
      if (!points.length) return '';
      return points.map((p, i) => `${i === 0 ? 'M' : 'L'} ${x(p)} ${y(p)}`).join(' ');
    }

    function simulate({ realFlow, legalFlow, hours, pulseVolume }) {
      const points = [];
      let prevTotalPulses = 0;
      let internalRemainderLiters = 0;

      for (let h = 1; h <= hours; h++) {
        const realLitersHour = realFlow * 3600;
        const realCumulativeLiters = realLitersHour * h;
        const totalPulses = Math.floor(realCumulativeLiters / pulseVolume);
        const pulsesThisHour = totalPulses - prevTotalPulses;
        const reportedFlow = (pulsesThisHour * pulseVolume) / 3600;
        const reportedCumulativeM3 = (totalPulses * pulseVolume) / 1000;
        const realCumulativeM3 = realCumulativeLiters / 1000;
        internalRemainderLiters = realCumulativeLiters - totalPulses * pulseVolume;

        points.push({
          hour: h,
          realFlow,
          legalFlow,
          realLitersHour,
          pulsesThisHour,
          reportedFlow,
          realCumulativeM3,
          reportedCumulativeM3,
          internalRemainderLiters,
          overLine: reportedFlow > legalFlow,
          realOverLegal: realFlow > legalFlow
        });

        prevTotalPulses = totalPulses;
      }

      const realVolumePeriod = realFlow * 3600 * hours / 1000;
      const legalVolumePeriod = legalFlow * 3600 * hours / 1000;
      const reportedVolumePeriod = points.length ? points[points.length - 1].reportedCumulativeM3 : 0;
      const hoursOverLimit = points.filter(p => p.overLine).length;

      return { points, realVolumePeriod, legalVolumePeriod, reportedVolumePeriod, hoursOverLimit };
    }

    function drawAxes(svg, width, height, margin, yMax, xMax, yLabel) {
      const innerW = width - margin.left - margin.right;
      const innerH = height - margin.top - margin.bottom;
      const x = (v) => margin.left + (v - 1) * (innerW / Math.max(1, xMax - 1));
      const y = (v) => margin.top + innerH - (v / yMax) * innerH;

      let out = '';
      out += `<rect x="0" y="0" width="${width}" height="${height}" fill="transparent"></rect>`;

      const yTicks = 6;
      for (let i = 0; i <= yTicks; i++) {
        const v = yMax * i / yTicks;
        const yy = y(v);
        out += `<line x1="${margin.left}" y1="${yy}" x2="${width - margin.right}" y2="${yy}" stroke="rgba(255,255,255,.08)" />`;
        out += `<text x="${margin.left - 10}" y="${yy + 4}" text-anchor="end" fill="#aeb8d8" font-size="12">${v.toFixed(yMax > 10 ? 0 : 2)}</text>`;
      }

      const xTickEvery = xMax <= 24 ? 1 : (xMax <= 48 ? 2 : 4);
      for (let i = 1; i <= xMax; i += xTickEvery) {
        const xx = x(i);
        out += `<line x1="${xx}" y1="${margin.top}" x2="${xx}" y2="${height - margin.bottom}" stroke="rgba(255,255,255,.05)" />`;
        out += `<text x="${xx}" y="${height - margin.bottom + 18}" text-anchor="middle" fill="#aeb8d8" font-size="12">${i}</text>`;
      }

      out += `<line x1="${margin.left}" y1="${height - margin.bottom}" x2="${width - margin.right}" y2="${height - margin.bottom}" stroke="#d5defa" />`;
      out += `<line x1="${margin.left}" y1="${margin.top}" x2="${margin.left}" y2="${height - margin.bottom}" stroke="#d5defa" />`;
      out += `<text x="${width / 2}" y="${height - 10}" text-anchor="middle" fill="#eaf0ff" font-size="13">Tiempo (horas)</text>`;
      out += `<text x="18" y="${height / 2}" text-anchor="middle" fill="#eaf0ff" font-size="13" transform="rotate(-90, 18, ${height / 2})">${yLabel}</text>`;

      return { x, y, markup: out };
    }

    function drawMainChart(points) {
      const svg = $('mainChart');
      const width = 1100, height = 480;
      const margin = { top: 24, right: 30, bottom: 42, left: 68 };
      const yMaxData = Math.max(...points.flatMap(p => [p.realFlow, p.legalFlow, p.reportedFlow]), 1);
      const yMax = yMaxData < 2 ? 2 : yMaxData * 1.18;
      const { x, y, markup } = drawAxes(svg, width, height, margin, yMax, points.length, 'Caudal (L/s)');

      const realPts = points.map(p => ({ x: p.hour, y: p.realFlow }));
      const legalPts = points.map(p => ({ x: p.hour, y: p.legalFlow }));
      const reportedPts = points.map(p => ({ x: p.hour, y: p.reportedFlow }));

      let out = markup;
      out += `<path d="${getLinePath(legalPts, p => x(p.x), p => y(p.y))}" fill="none" stroke="var(--legal)" stroke-width="3" stroke-dasharray="8 6" />`;
      out += `<path d="${getLinePath(realPts, p => x(p.x), p => y(p.y))}" fill="none" stroke="var(--real)" stroke-width="3" />`;
      out += `<path d="${getStepPath(reportedPts, p => x(p.x), p => y(p.y))}" fill="none" stroke="var(--reported)" stroke-width="3" />`;

      for (const p of reportedPts) {
        out += `<circle cx="${x(p.x)}" cy="${y(p.y)}" r="4.2" fill="var(--reported)" />`;
      }

      svg.innerHTML = out;
    }

    function drawAccChart(points, legalVolumePeriod) {
      const svg = $('accChart');
      const width = 1100, height = 320;
      const margin = { top: 22, right: 30, bottom: 42, left: 78 };
      const yMaxData = Math.max(...points.flatMap(p => [p.realCumulativeM3, p.reportedCumulativeM3]), legalVolumePeriod, 1);
      const yMax = yMaxData * 1.12;
      const { x, y, markup } = drawAxes(svg, width, height, margin, yMax, points.length, 'Volumen acumulado (m³)');

      const realPts = points.map(p => ({ x: p.hour, y: p.realCumulativeM3 }));
      const repPts = points.map(p => ({ x: p.hour, y: p.reportedCumulativeM3 }));
      const legPts = points.map((p) => ({ x: p.hour, y: (p.legalFlow * 3600 * p.hour) / 1000 }));

      let out = markup;
      out += `<path d="${getLinePath(legPts, p => x(p.x), p => y(p.y))}" fill="none" stroke="var(--legal)" stroke-width="3" stroke-dasharray="8 6" />`;
      out += `<path d="${getLinePath(realPts, p => x(p.x), p => y(p.y))}" fill="none" stroke="var(--real)" stroke-width="3" />`;
      out += `<path d="${getStepPath(repPts, p => x(p.x), p => y(p.y))}" fill="none" stroke="var(--reported)" stroke-width="3" />`;

      svg.innerHTML = out;
    }

    function updateTable(points, legalFlow) {
      const tbody = $('detailTable');
      tbody.innerHTML = points.map(p => {
        let dx = 'Operación normal';
        if (p.overLine && p.realFlow <= legalFlow) dx = 'Pico aparente por discretización';
        if (p.realFlow > legalFlow) dx = 'Sobreconsumo real';
        return `
          <tr>
            <td>${p.hour}</td>
            <td>${fmt.liters(p.realLitersHour)}</td>
            <td>${p.pulsesThisHour}</td>
            <td>${fmt.ls(p.reportedFlow)}</td>
            <td>${fmt.m3(p.realCumulativeM3)}</td>
            <td>${fmt.m3(p.reportedCumulativeM3)}</td>
            <td>${fmt.liters(p.internalRemainderLiters)}</td>
            <td>${dx}</td>
          </tr>
        `;
      }).join('');
    }

    function updateStatus(summary, realFlow, legalFlow) {
      const box = $('statusBox');
      const realExcess = summary.realVolumePeriod > summary.legalVolumePeriod + 1e-9;
      const apparentOnly = summary.hoursOverLimit > 0 && !realExcess;

      if (realExcess) {
        box.className = 'alert warn';
        box.textContent = `¡ALERTA DE SOBRECONSUMO REAL! El volumen real del período (${fmt.m3(summary.realVolumePeriod)}) supera el máximo legal (${fmt.m3(summary.legalVolumePeriod)}). La bomba opera sobre el límite (${fmt.ls(realFlow)} > ${fmt.ls(legalFlow)}).`;
      } else if (apparentOnly) {
        box.className = 'alert ok';
        box.textContent = `CUMPLIMIENTO LEGAL OK. Existen ${summary.hoursOverLimit} hora(s) con pico visible sobre el límite, pero el volumen real acumulado sigue dentro del máximo legal. Esto corresponde a un efecto de pulsos discretos, no a un incumplimiento real.`;
      } else {
        box.className = 'alert ok';
        box.textContent = `CUMPLIMIENTO LEGAL OK. El caudal real y el volumen acumulado se mantienen dentro del límite definido, sin evidencia de sobreconsumo real.`;
      }
    }

    function render() {
      $('realFlowValue').textContent = fmt.ls(state.realFlow);
      $('legalFlowValue').textContent = fmt.ls(state.legalFlow);
      $('hoursValue').textContent = `${state.hours} horas`;
      syncNumericInputs();

      const result = simulate(state);

      $('realVolumePeriod').textContent = fmt.m3(result.realVolumePeriod);
      $('legalVolumePeriod').textContent = fmt.m3(result.legalVolumePeriod);
      $('reportedVolumePeriod').textContent = fmt.m3(result.reportedVolumePeriod);
      $('hoursOverLimit').textContent = result.hoursOverLimit;

      drawMainChart(result.points);
      drawAccChart(result.points, result.legalVolumePeriod);
      updateTable(result.points, state.legalFlow);
      updateStatus(result, state.realFlow, state.legalFlow);
    }

    $('realFlow').addEventListener('input', (e) => {
      state.realFlow = Number(e.target.value);
      render();
    });

    $('legalFlow').addEventListener('input', (e) => {
      state.legalFlow = Number(e.target.value);
      render();
    });

    $('realFlowInput').addEventListener('input', (e) => {
      const value = Number(e.target.value);
      if (Number.isNaN(value)) return;
      state.realFlow = clamp(value, 0.1, 220);
      $('realFlow').value = state.realFlow;
      render();
    });

    $('legalFlowInput').addEventListener('input', (e) => {
      const value = Number(e.target.value);
      if (Number.isNaN(value)) return;
      state.legalFlow = clamp(value, 0.1, 220);
      $('legalFlow').value = state.legalFlow;
      render();
    });

    $('hours').addEventListener('input', (e) => {
      state.hours = Number(e.target.value);
      render();
    });

    $('pulseVolume').addEventListener('change', (e) => {
      state.pulseVolume = Number(e.target.value);
      render();
    });

    render();
  </script>
</body>
</html>
"""

# Renderizamos el HTML dentro de Streamlit (le damos bastante altura y permitimos scroll)
components.html(html_codigo, height=2200, scrolling=True)
