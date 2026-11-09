import { expectLogic } from 'kea-test-utils'

import { lemonToast } from '@posthog/lemon-ui'

import { StepInputRefusal } from 'scenes/hog-functions/configuration/inputRefusal'
import { secretReentryDialogLogic } from 'scenes/hog-functions/configuration/secretReentryDialogLogic'

import { useMocks } from '~/mocks/jest'
import { initKeaTests } from '~/test/init'

import { HogFlow } from './hogflows/types'
import { workflowLogic } from './workflowLogic'
import { workflowSecretReentryDialogKey, workflowSecretReentryLogic } from './workflowSecretReentryLogic'

const WORKFLOW_ID = 'wf-secret-reentry'
const PROPS = { id: WORKFLOW_ID }

const WORKFLOW: HogFlow = {
    id: WORKFLOW_ID,
    name: 'Secret re-entry',
    actions: [
        {
            id: 'trigger_node',
            type: 'trigger',
            name: 'Trigger',
            description: '',
            created_at: 0,
            updated_at: 0,
            config: { type: 'event', filters: {} },
        },
        {
            id: 'webhook',
            type: 'function',
            name: 'Send webhook',
            description: '',
            created_at: 0,
            updated_at: 0,
            config: {
                template_id: 'template-webhook',
                inputs: {
                    url: { value: 'https://new.example.com' },
                    api_key: { secret: true, value: undefined },
                    connection: { value: 1 },
                },
            },
        },
    ],
    edges: [{ from: 'trigger_node', to: 'webhook', type: 'continue' }],
    conversion: { filters: [] },
    exit_condition: 'exit_only_at_end',
    version: 1,
    status: 'draft',
    team_id: 1,
    trigger: { type: 'event', filters: {} } as HogFlow['trigger'],
    created_at: '2026-05-01T00:00:00.000Z',
    updated_at: '2026-05-01T00:00:00.000Z',
} as HogFlow

const TEMPLATE = {
    id: 'template-webhook',
    name: 'Webhook',
    type: 'destination',
    inputs_schema: [
        { key: 'url', type: 'string', label: 'Webhook URL', required: true },
        { key: 'api_key', type: 'string', label: 'API key', secret: true, required: false },
        { key: 'connection', type: 'integration', integration: 'slack', label: 'Slack', required: false },
    ],
}

const SECRET_DETAIL =
    'This change sends API key to new.example.com. Until now it was only sent to old.example.com. Enter it again to confirm.'
const CONNECTION_DETAIL = 'Slack connections can only send credentials to slack.com.'

const refusal = (code: string, key: string, detail: string): Record<string, string> => ({
    type: 'validation_error',
    code,
    attr: `actions__webhook__inputs__${key}`,
    detail,
})

const stepRefusal = (key: string): StepInputRefusal => ({
    stepId: 'webhook',
    key,
    detail: SECRET_DETAIL,
    code: 'secret_reentry_required',
})

describe('workflowSecretReentryLogic', () => {
    let logic: ReturnType<typeof workflowLogic.build>
    let dialog: ReturnType<typeof secretReentryDialogLogic.build>
    let patchBodies: Record<string, any>[]
    let refusals: Record<string, string>[]

    beforeEach(async () => {
        patchBodies = []
        refusals = []
        useMocks({
            get: {
                '/api/environments/:team_id/hog_flows/:id/': WORKFLOW,
                '/api/projects/:team_id/hog_function_templates/': { results: [TEMPLATE], count: 1 },
                '/api/environments/:team_id/hog_flows/:id/schedules/': [],
            },
            patch: {
                '/api/environments/:team_id/hog_flows/:id/': async ({ request }) => {
                    patchBodies.push((await request.json()) as Record<string, any>)
                    const next = refusals.shift()
                    return next ? [400, next] : [200, WORKFLOW]
                },
            },
        })
        jest.spyOn(lemonToast, 'error')
        initKeaTests()
        logic = workflowLogic(PROPS)
        logic.mount()
        workflowSecretReentryLogic(PROPS).mount()
        dialog = secretReentryDialogLogic({ dialogKey: workflowSecretReentryDialogKey(PROPS) })
        dialog.mount()
        await expectLogic(logic).toDispatchActions(['loadWorkflowSuccess'])
        await expectLogic(logic).toFinishAllListeners()
    })

    afterEach(() => {
        jest.useRealTimers()
        jest.restoreAllMocks()
    })

    it('asks for a refused secret on an explicit save, and saves the entered value', async () => {
        refusals.push(refusal('secret_reentry_required', 'api_key', SECRET_DETAIL))
        logic.actions.setWorkflowValue('name', 'Renamed')

        await expectLogic(logic, () => logic.actions.submitWorkflow()).toDispatchActions(['saveWorkflowFailure'])
        await expectLogic(dialog).toDispatchActions(['openSecretReentry'])

        expect(dialog.values.prompt).toMatchObject({ label: 'API key in Send webhook', detail: SECRET_DETAIL })
        expect(lemonToast.error).not.toHaveBeenCalled()

        dialog.actions.setSecretValue('new key')
        await expectLogic(logic, () => dialog.actions.confirmSecretReentry()).toDispatchActions(['saveWorkflowSuccess'])

        expect(dialog.values.isOpen).toBe(false)
        const sent = patchBodies[patchBodies.length - 1].actions.find((action: any) => action.id === 'webhook')
        expect(sent.config.inputs.api_key.value).toEqual('new key')
    })

    it('marks the step on a refused auto-save and asks only when the user saves', async () => {
        jest.useFakeTimers()
        refusals.push(refusal('secret_reentry_required', 'api_key', SECRET_DETAIL))
        logic.actions.setWorkflowValue('name', 'Renamed')
        await jest.advanceTimersByTimeAsync(3500)
        await expectLogic(logic).toDispatchActions(['saveWorkflowFailure'])

        expect(dialog.values.isOpen).toBe(false)
        expect(logic.values.actionValidationErrorsById['webhook']).toMatchObject({
            valid: false,
            errors: { api_key: SECRET_DETAIL },
        })
        expect(lemonToast.error).not.toHaveBeenCalled()

        logic.actions.submitWorkflow()
        expect(dialog.values.isOpen).toBe(true)
    })

    it('keeps publishing when the save of an entered secret is refused for another secret', async () => {
        logic.actions.setStepInputRefusal(stepRefusal('api_key'))
        logic.actions.publishDraft()
        expect(dialog.values.prompt?.source).toEqual('publish')

        refusals.push(refusal('secret_reentry_required', 'api_key', SECRET_DETAIL))
        dialog.actions.setSecretValue('first key')
        await expectLogic(logic, () => dialog.actions.confirmSecretReentry()).toDispatchActions(['saveWorkflowFailure'])
        expect(dialog.values.prompt?.source).toEqual('publish')

        dialog.actions.setSecretValue('second key')
        await expectLogic(logic, () => dialog.actions.confirmSecretReentry()).toDispatchActions([
            'saveWorkflowSuccess',
            'publishDraft',
        ])
    })

    it('forgets refused secrets when the changes are discarded', async () => {
        logic.actions.setStepInputRefusal(stepRefusal('api_key'))
        logic.actions.resetWorkflow(logic.values.originalWorkflow ?? undefined)

        expect(logic.values.actionValidationErrorsById['webhook']?.errors?.api_key).toBeUndefined()
        logic.actions.submitWorkflow()
        expect(dialog.values.isOpen).toBe(false)
    })

    it('shows a refused connection on its input, with no dialog and no toast', async () => {
        refusals.push(refusal('integration_origin_refused', 'connection', CONNECTION_DETAIL))
        logic.actions.setWorkflowValue('name', 'Renamed')

        await expectLogic(logic, () => logic.actions.submitWorkflow()).toDispatchActions(['saveWorkflowFailure'])

        expect(logic.values.actionValidationErrorsById['webhook']?.errors).toMatchObject({
            connection: CONNECTION_DETAIL,
        })
        expect(dialog.values.isOpen).toBe(false)
        expect(lemonToast.error).not.toHaveBeenCalled()
    })
})
