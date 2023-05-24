import argparse

from utils import session_mode, ScrappingMode
from feeds import DayZeroOrchestrator, NasdaqOrchestrator, YahooOrchestrator, BizInsiderOrchestrator


def prepare_args(args):
    refactored_args = {"domain_orchestrator":None,"reset_session":None}
    refactored_args["domain_orchestrator"] = args.domain_orchestrator
    if args.scrapping_mode == 0:
        refactored_args['scrapping_mode'] = ScrappingMode.current
    elif args.scrapping_mode == 1:
        refactored_args['scrapping_mode'] = ScrappingMode.history
    ## Implement for restart_mode
    refactored_args["reset_session"] = args.reset_session
    return refactored_args


@session_mode(restart=False, scrapping_mode=ScrappingMode.history)
def main(instruct=None):
    orchestrators = {
        0: DayZeroOrchestrator,
        1: NasdaqOrchestrator,
        2: YahooOrchestrator,
        3: BizInsiderOrchestrator
    }
    orchestrator_name = {
        0: "Day 0 ",
        1: "Nasdaq",
        2: "Yahoo",
        3: "Business Insider"
    }

    orchestrator_to_run = orchestrators[instruct['domain_orchestrator']]()
    orch_name = orchestrator_name[instruct['domain_orchestrator']]
    print(f"RUNNING ORCHESTRATOR {orch_name}")
    print(instruct)
    orchestrator_to_run.run(instructions=instruct)


if __name__ == "__main__":
    help_domain = """
    Choose number from 1 to 4. Each of them corresponds to those orchestrators:
    0 - Day 0 
    1 - Nasdaq
    2 - Yahoo
    3 - Business Insider 
    """
    help_scrapping_mode = """
    Choose number from 1 to 2 to choose the scrapping mode:
    0 - history (it scrapes all the history for the domain)
    1 - current (it scrapes only the current newest data)
    """
    help_reset_session = """
    Choose if you want to restart your session:
    True - restart session and get a new token
    False - do not restart session and use old token
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--domain-orchestrator", type=int, help=help_domain,
                        choices=range(0, 4))
    parser.add_argument("-s", "--scrapping-mode", type=int, help=help_scrapping_mode,
                        choices=range(0, 2))
    parser.add_argument("-r", "--reset-session", type=bool, help=help_reset_session)
    args = parser.parse_args()
    main(instruct=prepare_args(args=args))
