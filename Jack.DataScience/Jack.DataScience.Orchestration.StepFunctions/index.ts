import { StepFunctions } from 'aws-sdk';



 

 

export class StepFunctionsBuilder {
    public options: StepFunctions.ClientConfiguration = <any>{};
    public stepFunctions: StepFunctions;
    public constructor(){
        this.options = eval(`(${process.env['awsoptions']})`);
        this.stepFunctions = new StepFunctions(this.options);
    }
    public createStateMachine(){
        let input: StepFunctions.CreateStateMachineInput = <any> {};
        input.name = 'LogicFlowMachine';
        input.roleArn = '';
        this.stepFunctions.createStateMachine();
    }
}