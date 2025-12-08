### A5 ###
import pandas as pd
from datetime import datetime, timedelta
from pyomo.environ import ConcreteModel, Var, Param, Set, NonNegativeReals, NonNegativeIntegers, Any, Constraint, Objective, SolverFactory, value, minimize, SolverStatus, TerminationCondition
import pandas as pd
import pytz

def generate_schedule(lastProductionPrognosis, 
                    lastConsumptionPrognosis, 
                    lastNpSpotPricePrognosis, 
                    npSpotCurrentPrice, 
                    lastEss_e_lt, 
                    ess_p ,
                    ess_charge,
                    ess_soc,
                    ess_max_p,
                    ess_max_e,
                    ess_charge_end = 10*1000,
                    ess_soc_min = 0,
                    ess_soc_max = 0,
                    ess_safe_min = 0.1,
                    pccImportLimitW = 20000,
                    pccExportLimitW = -15000,
                    startTime = datetime.now(),
                    endTime = datetime.now() + timedelta(seconds=86400), # +24h
                    interval = 900, #15min
                    DAY_TARIFF = 0.07,
                    NIGHT_TARIFF = 0.05,
                    ESS_DEG_COST = 0.139,
                    local_timezone = pytz.timezone('Europe/Tallinn'),
                    logger = None):

    # Get production and consumption forecasts
    productionPrognosis = pd.DataFrame(lastProductionPrognosis) 
    productionPrognosis = productionPrognosis.set_index('time')
    productionPrognosis.index = pd.to_datetime(productionPrognosis.index, utc=True)

    consumptionPrognosis = pd.DataFrame(lastConsumptionPrognosis)
    consumptionPrognosis = consumptionPrognosis.set_index('time')
    consumptionPrognosis.index = pd.to_datetime(consumptionPrognosis.index, utc=True)

    # Get existing ess plan
    try:
        ess_e_lt = pd.DataFrame(lastEss_e_lt)
        ess_e_lt = ess_e_lt.set_index('time')
        ess_e_lt.index = pd.to_datetime(ess_e_lt.index, utc=True)
    except Exception as e:
        ess_e_lt = pd.DataFrame(index=productionPrognosis.index)
        ess_e_lt['value'] = 0

    # Get NP prices for the future
    npSpotPricePrognosis = pd.DataFrame(lastNpSpotPricePrognosis)
    npSpotPricePrognosis = npSpotPricePrognosis.set_index('time')
    npSpotPricePrognosis.index = pd.to_datetime(npSpotPricePrognosis.index, utc=True)

    # Insert NP price of current hour into dataset
    currentHour = datetime.now().strftime("%Y-%m-%dT%H:00:00Z")
    npSpotPricePrognosis.at[currentHour,'value'] = npSpotCurrentPrice
    npSpotPricePrognosis = npSpotPricePrognosis.sort_index()
    
    # Insert NP price of current hour into dataset
    currentHour = datetime.now().strftime("%Y-%m-%dT%H:00:00Z")
    npSpotPricePrognosis.at[currentHour,'value'] = npSpotCurrentPrice
    npSpotPricePrognosis = npSpotPricePrognosis.sort_index()

    # Log parameters
    logger.info(f'Scheduling initialised with following parameters: n\
        ESS P = {ess_p/1000} kW\n\
        ESS available charge = {ess_charge/1000} kWh\n\
        ESS charge at end time = {ess_charge_end/1000} kWh\n\
        ESS SOC = {ess_soc*100:.1f} %\n\
        ESS MAX P = {ess_max_p/1000} kW\n\
        ESS MAX E = {ess_max_e/1000} kWh\n\
        ESS SOC MIN = {ess_soc_min:.1f} %\n\
        ESS SOC MAX = {ess_soc_max:.1f} %\n\
        ESS MIN SAFE LIM = {ess_safe_min:.1f} %\n\
        PCC MAX IMPORT = {pccImportLimitW/1000} kW\n\
        PCC MAX EXPORT = {pccExportLimitW/1000} kW\n\
        Start time = {startTime}\n\
        End time = {endTime}\n\
        Day Tariff = {DAY_TARIFF} €\n\
        Night Tariff = {NIGHT_TARIFF} €\n\
        ESS degradation cost = {ESS_DEG_COST} €/kWh\n\
        Timezone = {local_timezone}\n\
        Interval = {interval/60} min')

    ########### Prepare dataset with consumption, production, spot price, pcc, cost and tariff ###############
    dataset = npSpotPricePrognosis.copy().drop(columns=['id','datapointPrognosisId'])
    dataset = dataset.rename(columns={'value': 'spotprice'})
    dataset['consumption'] = None
    dataset['production'] = None
    dataset['pcc'] = None
    dataset['cost'] = None
    dataset['tariff'] = None

    # Ensure index is UTC
    dataset.index = pd.to_datetime(dataset.index, utc=True)
    
    for index in dataset.index:
        if index >  consumptionPrognosis.index[0]:

            nearest_index = consumptionPrognosis.index.asof(index)
            dataset.at[index,'consumption'] = consumptionPrognosis.loc[nearest_index, 'value']
            dataset.at[index,'production'] = productionPrognosis.loc[nearest_index, 'value']
            dataset.at[index,'pcc'] = consumptionPrognosis.loc[nearest_index, 'value'] + productionPrognosis.loc[nearest_index, 'value']
            GRID_TARIFF_COMPONENT = NIGHT_TARIFF if (5 <= index.weekday() <= 6) or (index.hour >= 22 or index.hour < 7) else DAY_TARIFF
            dataset.at[index,'cost'] = dataset.loc[index,'spotprice']/1000 * dataset.loc[index,'pcc']/1000 * (interval/3600) if dataset.loc[index,'pcc'] < 0 else (dataset.loc[index,'spotprice']/1000 + GRID_TARIFF_COMPONENT) * dataset.loc[index,'pcc']/1000 * (interval/3600)
            dataset.at[index,'tariff'] = GRID_TARIFF_COMPONENT
        else:
            dataset = dataset.drop(index)

    # Convert to local timezone
    dataset.index = dataset.index.tz_convert(local_timezone)

    ########################### Extract and validate input #################################
    prod = dataset['production'].values 
    cons = dataset['consumption'].values 
    np = dataset['spotprice'].values
    tf = dataset['tariff'].values

    assert len(prod) == len(cons), "len(prod) != len(cons)"
    assert len(prod) == len(np), "len(prod) != len(np)"
    assert len(prod) == len(tf), "len(prod) != len(tf)"

    ############################ Parametrize model #############################
    ESS_kW = ess_max_p                 # salvesti võimsus
    ESS_kWh = ess_charge               # salvesti efektiivne laetus optimeerimise alguses kWh-des (võib olla negatiivne)
    ESS_max_kWh = ess_max_e            # salvesti absoluutne mahutavus
    P_imp_lim_kW = pccImportLimitW     # pcc max tarbimine
    P_exp_lim_kW = pccExportLimitW     # pcc max müük/tootlus (negatiivne)
    ESS_SOC_min = ess_soc_min               # within safe limits
    ESS_SOC_max = ess_soc_max
    ESS_safe_min = ess_safe_min
    kW_to_kWh = interval/3600               # kordaja võimsuse teisendamiseks energiaks (intervallist tulenev)
    ESS_END_kWh = ess_charge_end       # salvesti efektiivne laetus optimeerimise lõpus kWh-des

    ESS_eff_kWh = ESS_max_kWh               #(ESS_max_kWh*(ESS_SOC_max - ESS_SOC_min)/100) # Salvesti efektiivne mahutavus
    ESS_SOC_0 = (ESS_kWh/ESS_eff_kWh)*100   #+ ESS_safe_min
    ESS_SOC_0 = 0 if ESS_SOC_0 <0 else ESS_SOC_0
    ESS_SOC_END = ESS_END_kWh/ESS_eff_kWh*100
    ########################################################################


    ########################### debug info #################################
    logger.info(f'ESS_kW = {ESS_kW} kW\n\
        ESS_kWh = {ESS_kWh} kWh\n\
        ESS_max_kWh = {ESS_max_kWh} kWh\n\
        P_imp_lim_kW = {P_imp_lim_kW} kW\n\
        P_exp_lim_kW = {P_exp_lim_kW} kW\n\
        ESS_SOC_min = {ESS_SOC_min} %\n\
        ESS_SOC_max = {ESS_SOC_max} %\n\
        ESS_safe_min = {ESS_safe_min} %\n\
        kW_to_kWh = {kW_to_kWh} \n\
        ESS_END_kWh = {ESS_END_kWh} kWh\n\
        ESS_eff_kWh = {ESS_eff_kWh} kWh\n\
        ESS_SOC_0 = {ESS_SOC_0} %\n\
        ESS_SOC_END = {ESS_SOC_END} %')
    
    logger.debug(f'Production: {prod}')
    logger.debug(f'Consumption: {cons}')
    logger.debug(f'Spot price: {np}')
    logger.debug(f'Tariff: {tf}')
    ########################################################################

    ########################### Method for Converting Model Data to Pandas DataFrame #################################
    def model_to_df(m):
        st = 0
        en = len(m.T)

        periods = range(st, en)
        load = [value(m.P_kW[i]) for i in periods]
        pv = [value(m.PV_kW[i]) for i in periods]
        pcc_export_kW = [value(m.PCC_EXPORT_kW[i]) for i in periods]
        pcc_import_kW = [value(m.PCC_IMPORT_kW[i]) for i in periods]
        ess = [value(m.ESS_kW[i]) for i in periods]
        ess_soc = [value(m.ESS_SoC[i]) for i in periods]    
        pcc = [value(m.PCC_IMPORT_kW[i])-value(m.PCC_EXPORT_kW[i]) for i in periods]
        spot = [value(m.SPOT_EUR_kWh[i]) for i in periods]  
        tariff = [value(m.TARIFF_EUR_kWh[i]) for i in periods] 

        df_dict = {
            'Period': periods,
            'Load': load,
            'PV': pv,
            'ESS': ess,
            'ESS effective SoC': ess_soc,
            'PCC Export': pcc_export_kW,
            'PCC Import': pcc_import_kW,
            'PCC': pcc,
            'Spot Price': spot,
            'Grid tariff': tariff
        }

        df = pd.DataFrame(df_dict)

        return df

    ########################### Build and Solve Model #################################
    #### Prepare data as DataFrame
    data = pd.DataFrame({
        'Load': cons,
        'PV': prod,
        'Spot': np,
        'Tariff': tf
    })
    
    #### Initiate Model
    m = ConcreteModel()

    #### Define Fixed Model Parameters
    m.T = Set(initialize=data.index.tolist(), doc='Indexes', ordered=True)
    m.P_kW = Param(m.T, initialize=data.Load, doc='Load [kW]', within=Any)
    m.PV_kW = Param(m.T, initialize=data.PV, doc='PV generation [kW]', within=Any)
    m.SPOT_EUR_kWh = Param(m.T, initialize=data.Spot, doc='Spot Market Price [€/kWh]', within=Any)
    m.TARIFF_EUR_kWh = Param(m.T, initialize=data.Tariff, doc='Grid Tariff [€/kWh]', within=Any)

    #### Define Variable Model Parameters
    m.PCC_exp_z = Var(m.T, bounds=(0,1), within=NonNegativeIntegers)
    m.PCC_imp_z = Var(m.T, bounds=(0,1), within=NonNegativeIntegers)
    m.PCC_EXPORT_kW = Var(m.T, within=NonNegativeReals)
    m.PCC_IMPORT_kW = Var(m.T, within=NonNegativeReals)
    m.ESS_kW = Var(m.T, bounds=(-ESS_kW, ESS_kW), doc='ESS P [kW]')
    m.ESS_kW_charge = Var(m.T, within=NonNegativeReals, doc='ESS P charge [kW]')
    m.ESS_kW_discharge = Var(m.T, within=NonNegativeReals, doc='ESS P discharge [kW]')
    m.ESS_kW_charge_z = Var(m.T, bounds=(0,1), within=NonNegativeIntegers)
    m.ESS_kW_discharge_z = Var(m.T, bounds=(0,1), within=NonNegativeIntegers)
    m.ESS_SoC = Var(m.T, bounds=(0, 100), initialize=ESS_SOC_0, doc='ESS effective SoC [%]',  within=NonNegativeReals)

    #### Define Model Rules and Constraints
    def sim_import_export_restrict_rule(m, t):
        "Prohibit simultaneous PCC export and import"
        return m.PCC_exp_z[t] + m.PCC_imp_z[t] <= 1

    m.sim_import_export_restrict = Constraint(m.T, rule=sim_import_export_restrict_rule)

    def pcc_export_kW_rule(m, t):
        "PCC export calculation"
        return m.PCC_EXPORT_kW[t] <= -P_exp_lim_kW*m.PCC_exp_z[t]

    m.pcc_export_kW = Constraint(m.T, rule=pcc_export_kW_rule)

    def pcc_import_kW_rule(m, t):
        "PCC import calculation"
        return m.PCC_IMPORT_kW[t] <= P_imp_lim_kW*m.PCC_imp_z[t]

    m.pcc_import_kW = Constraint(m.T, rule=pcc_import_kW_rule)

    def sim_charge_discharge_restrict_rule(m, t):
        "Prohibit ESS simultaneous charging and discharging"
        return m.ESS_kW_charge_z[t] + m.ESS_kW_discharge_z[t] <= 1

    m.sim_charge_discharge_restrict = Constraint(m.T, rule=sim_charge_discharge_restrict_rule)

    def ESS_kW_charge_rule(m, t):
        "ESS charge kW Calculation"
        return m.ESS_kW_charge[t] <= ESS_kW*m.ESS_kW_charge_z[t]

    m.ESS_kW_charging = Constraint(m.T, rule=ESS_kW_charge_rule)

    def ESS_kW_discharge_rule(m, t):
        "ESS discharge kW Calculation"
        return m.ESS_kW_discharge[t] <= ESS_kW*m.ESS_kW_discharge_z[t]

    m.ESS_kW_discharging = Constraint(m.T, rule=ESS_kW_discharge_rule)

    def ESS_kW_calc_rule(m, t):
        "ESS kW Calculation"
        return m.ESS_kW[t] == m.ESS_kW_charge[t] - m.ESS_kW_discharge[t]

    m.ESS_kW_calculation = Constraint(m.T, rule=ESS_kW_calc_rule)

    def ess_SoC_rule(m, t):
        "ESS SOC Calculation"
        if t >= 1:
            return m.ESS_SoC[t] == m.ESS_SoC[t-1] + ((m.ESS_kW[t-1]*kW_to_kWh)/ESS_eff_kWh)*100
        else:
            return m.ESS_SoC[t] == ESS_SOC_0

    m.ess_SoC_const = Constraint(m.T, rule=ess_SoC_rule)

    def soc_end_target_rule(m):
        "End SoC value"
        return m.ESS_SoC[len(m.ESS_SoC)-1] + ((m.ESS_kW[len(m.ESS_SoC)-1]*kW_to_kWh)/ESS_eff_kWh)*100 == ESS_SOC_END

    m.soc_end_target = Constraint(rule=soc_end_target_rule)

    def pcc_self_consumption_rule(m, t):
        "Self consumption rule"
        return m.PCC_IMPORT_kW[t] - m.PCC_EXPORT_kW[t] == m.P_kW[t] + m.PV_kW[t] + m.ESS_kW[t]

    m.pcc_self_consumption = Constraint(m.T, rule=pcc_self_consumption_rule)

    #### Cost function and optimization objective
    # Cost function
    cost = sum(( (m.PCC_IMPORT_kW[t]/1000)*kW_to_kWh*(m.SPOT_EUR_kWh[t]/1000 + m.TARIFF_EUR_kWh[t]) - 
                (m.PCC_EXPORT_kW[t]/1000)*kW_to_kWh*m.SPOT_EUR_kWh[t]/1000 + 
                (m.ESS_kW_charge[t]/1000)*kW_to_kWh*ESS_DEG_COST ) for t in m.T) #
    m.objective = Objective(expr = cost, sense=minimize)

    # Solver
    solver = SolverFactory('glpk', options={'tmlim': 300} , executable=r'/usr/bin/glpsol')
    results = solver.solve(m, tee=True)


    if (results.solver.status == SolverStatus.ok) and ((results.solver.termination_condition == TerminationCondition.optimal) or (results.solver.termination_condition == TerminationCondition.feasible)):
        imp_kW = 0
        exp_kW = 0
        for i in m.PCC_EXPORT_kW:
            print(f"PCC_EXPORT_kW[{i}] = {m.PCC_EXPORT_kW[i]()/1000:.2f}; PCC_IMPORT_kW[{i}] = {m.PCC_IMPORT_kW[i]()/1000:.2f}; P_kW[{i}] = {m.P_kW[i]/1000:.2f}; PV_kW[{i}] = {m.PV_kW[i]/1000:.2f};" +
                f" ESS_C_kW[{i}] = {m.ESS_kW_charge[i]()/1000:.2f}; ESS_D_kW[{i}] = {m.ESS_kW_discharge[i]()/1000:.2f}; ESS_kW[{i}] = {m.ESS_kW[i]()/1000:.2f};"+ 
                f" ESS SOC[{i}] = {m.ESS_SoC[i]():.1f};"+
                f" COST = IMP ({(m.PCC_IMPORT_kW[i]()/1000)*kW_to_kWh*(m.SPOT_EUR_kWh[i]/1000 + m.TARIFF_EUR_kWh[i]):.3f}) - EXP ({(m.PCC_EXPORT_kW[i]()/1000)*kW_to_kWh*m.SPOT_EUR_kWh[i]/1000:.3f}) + ESS ({(m.ESS_kW_charge[i]()/1000)*kW_to_kWh*ESS_DEG_COST:.3f})" +    
                f" = {(m.PCC_IMPORT_kW[i]()/1000)*kW_to_kWh*(m.SPOT_EUR_kWh[i]/1000 + m.TARIFF_EUR_kWh[i]) - (m.PCC_EXPORT_kW[i]()/1000)*kW_to_kWh*m.SPOT_EUR_kWh[i]/1000 + (m.ESS_kW_charge[i]()/1000)*kW_to_kWh*ESS_DEG_COST:.2f}")
        
            imp_kW += m.PCC_IMPORT_kW[i]()
            exp_kW += m.PCC_EXPORT_kW[i]()

        # Format results as data frame
        results_df = model_to_df(m)
        logger.info(f'<<< INITIAL COST = {dataset["cost"].sum():.2f} for {dataset["pcc"].sum()*kW_to_kWh/1000:.2f} kWh grid electricity >>> VS <<< TOTAL COST={value(m.objective()):.2f} for {(imp_kW-exp_kW)*kW_to_kWh/1000:.2f} kWh grid electricity>>>')
        load = results_df["ESS"].values
        
        logger.info(",".join(f"{x:.4g}" for x in load))

    else:    
        results_df = None
        logger.info(results.write())

    return results_df    