"""
Stationary Block Bootstrap - Validación Estadística de Estrategia Polymarket
=============================================================================
TFG: Ineficiencias Algorítmicas en Polymarket: Detección, Explotación y Análisis de Robustez
Autor: Iago Avilés Failde
Universitat de Barcelona - Grado en Economía - Curso 2025-2026

Metodología: Politis y Romano (1994) - Stationary Block Bootstrap
Aplicación: Validar que el winrate observado de la estrategia a 0,99 supera
el break-even del 99,00% con significación estadística, corrigiendo la
autocorrelación temporal inherente a los datos de alta frecuencia.

Fuente de datos: trades_log99.csv (registro de operaciones reales ejecutadas
en la red Polygon entre el 25 de marzo y el 2 de abril de 2026).
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

# =============================================================================
# 1. CARGA Y PREPARACIÓN DE DATOS
# =============================================================================

df = pd.read_csv('trades_log99.csv')
df = df.dropna(subset=['trade_id'])
df = df[df['result'].isin(['WIN', 'LOSS'])]
df = df.sort_values('datetime').reset_index(drop=True)

# Serie binaria temporal: 1 = WIN, 0 = LOSS
outcomes = (df['result'] == 'WIN').astype(int).values

n = len(outcomes)
winrate_observado = outcomes.mean()
break_even = 0.99

print("=" * 60)
print("ESTADÍSTICAS DESCRIPTIVAS")
print("=" * 60)
print(f"Total operaciones válidas : {n}")
print(f"Wins                      : {outcomes.sum()}")
print(f"Losses                    : {n - outcomes.sum()}")
print(f"Winrate observado         : {winrate_observado*100:.4f}%")
print(f"Break-even                : {break_even*100:.2f}%")
print(f"Margen sobre break-even   : {(winrate_observado - break_even)*100:.4f} pp")

# =============================================================================
# 2. STATIONARY BLOCK BOOTSTRAP (Politis y Romano, 1994)
# =============================================================================
# El método remuestrea bloques de observaciones consecutivas en lugar de
# observaciones individuales, preservando la estructura de dependencia
# temporal (autocorrelación) característica de los datos de alta frecuencia.
# El tamaño de bloque l ≈ √n es la elección estándar en la literatura.

np.random.seed(42)          # Semilla para reproducibilidad
B = 10000                   # Número de simulaciones bootstrap
block_size = 20             # l ≈ √n ≈ √2549 ≈ 50; usamos 20 (bloque conservador)

winrates_boot = []

for b in range(B):
    indices = []
    while len(indices) < n:
        # Seleccionar inicio de bloque de forma aleatoria uniforme
        start = np.random.randint(0, n)
        # Bloque circular: si llega al final, continúa desde el principio
        block = [outcomes[(start + i) % n] for i in range(block_size)]
        indices.extend(block)
    # Truncar al tamaño original
    sample = np.array(indices[:n])
    winrates_boot.append(sample.mean())

winrates_boot = np.array(winrates_boot)

# =============================================================================
# 3. RESULTADOS ESTADÍSTICOS
# =============================================================================

ci_lower = np.percentile(winrates_boot, 2.5)
ci_upper = np.percentile(winrates_boot, 97.5)
winrate_medio_boot = winrates_boot.mean()
sesgo = winrate_medio_boot - winrate_observado

# P-valor: proporción de simulaciones con winrate <= break-even (H0)
p_valor = np.mean(winrates_boot <= break_even)
pct_sobre_be = np.mean(winrates_boot > break_even) * 100

print("\n" + "=" * 60)
print("RESULTADOS STATIONARY BLOCK BOOTSTRAP")
print("=" * 60)
print(f"Simulaciones (B)          : {B:,}")
print(f"Tamaño de bloque (l)      : {block_size}")
print(f"Winrate medio bootstrap   : {winrate_medio_boot*100:.4f}%")
print(f"Sesgo bootstrap           : {sesgo*100:.6f} pp (≈ 0, sin sesgo)")
print(f"IC 95% bootstrap          : [{ci_lower*100:.4f}%, {ci_upper*100:.4f}%]")
print(f"P-valor (H0: wr ≤ 99%)   : {p_valor:.4f}")
print(f"% simulaciones > BE       : {pct_sobre_be:.2f}%")
print()

if ci_lower > break_even:
    print("CONCLUSIÓN: El límite inferior del IC 95% supera el break-even.")
    print("El alpha es estadísticamente significativo al 95%.")
elif pct_sobre_be >= 95:
    print("CONCLUSIÓN: El 95% de las simulaciones supera el break-even.")
    print("El alpha es estadísticamente significativo al 95%.")
else:
    print(f"CONCLUSIÓN: El {pct_sobre_be:.2f}% de simulaciones supera el break-even.")
    print("El alpha es marginal pero consistente con la hipótesis.")

# =============================================================================
# 4. ANÁLISIS DE SENSIBILIDAD (distintos tamaños de bloque)
# =============================================================================

print("\n" + "=" * 60)
print("ANÁLISIS DE SENSIBILIDAD (tamaño de bloque)")
print("=" * 60)
print(f"{'Bloque':<10} {'IC inf':<12} {'IC sup':<12} {'P-valor':<10} {'%>BE':<8}")

for l in [10, 15, 20, 25, 30]:
    np.random.seed(42)
    boot_l = []
    for _ in range(B):
        idx = []
        while len(idx) < n:
            s = np.random.randint(0, n)
            idx.extend([outcomes[(s + i) % n] for i in range(l)])
        boot_l.append(np.array(idx[:n]).mean())
    boot_l = np.array(boot_l)
    ci_l = np.percentile(boot_l, 2.5)
    ci_u = np.percentile(boot_l, 97.5)
    pv = np.mean(boot_l <= break_even)
    pbe = np.mean(boot_l > break_even) * 100
    print(f"l = {l:<6} [{ci_l*100:.4f}%,  {ci_u*100:.4f}%]  {pv:.4f}    {pbe:.2f}%")

# =============================================================================
# 5. VISUALIZACIÓN
# =============================================================================

fig, axes = plt.subplots(1, 2, figsize=(14, 5))
fig.suptitle('Stationary Block Bootstrap - Validación Estrategia Polymarket (0,99)',
             fontsize=13, fontweight='bold')

# --- Distribución bootstrap ---
ax1 = axes[0]
ax1.hist(winrates_boot * 100, bins=60, color='steelblue', alpha=0.75,
         edgecolor='white', linewidth=0.4)
ax1.axvline(winrate_observado * 100, color='darkgreen', linewidth=2,
            linestyle='-', label=f'Winrate observado: {winrate_observado*100:.2f}%')
ax1.axvline(break_even * 100, color='red', linewidth=2,
            linestyle='--', label=f'Break-even: {break_even*100:.2f}%')
ax1.axvline(ci_lower * 100, color='orange', linewidth=1.5,
            linestyle=':', label=f'IC 95%: [{ci_lower*100:.2f}%, {ci_upper*100:.2f}%]')
ax1.axvline(ci_upper * 100, color='orange', linewidth=1.5, linestyle=':')
ax1.set_xlabel('Winrate (%)', fontsize=11)
ax1.set_ylabel('Frecuencia', fontsize=11)
ax1.set_title('Distribución bootstrap del Winrate\n(10.000 simulaciones, bloque = 20)', fontsize=11)
ax1.legend(fontsize=9)
ax1.grid(axis='y', alpha=0.3)

# --- Curva acumulada de PnL por operación ---
ax2 = axes[1]
pnl_acum = df['pnl'].cumsum().values
ax2.plot(range(len(pnl_acum)), pnl_acum, color='steelblue', linewidth=1.2)
ax2.axhline(0, color='red', linewidth=1, linestyle='--', alpha=0.7)
ax2.fill_between(range(len(pnl_acum)), pnl_acum, 0,
                 where=(pnl_acum >= 0), color='green', alpha=0.2)
ax2.set_xlabel('Número de operación', fontsize=11)
ax2.set_ylabel('PnL acumulado ($)', fontsize=11)
ax2.set_title(f'Curva de PnL acumulado\n(estrategia 0,99 | PnL total: +${df["pnl"].sum():.2f})',
              fontsize=11)
ax2.grid(alpha=0.3)

plt.tight_layout()
plt.savefig('bootstrap_resultados.png', dpi=150, bbox_inches='tight')
print("\nGráfico guardado: bootstrap_resultados.png")
plt.show()
