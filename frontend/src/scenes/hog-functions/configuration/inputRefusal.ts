export const SECRET_REENTRY_CODE = 'secret_reentry_required'
export const CONNECTION_REFUSAL_CODE = 'integration_origin_refused'

export interface InputRefusal {
    key: string
    detail: string
}

export interface StepInputRefusal extends InputRefusal {
    stepId: string
    code: typeof SECRET_REENTRY_CODE | typeof CONNECTION_REFUSAL_CODE
}

const INPUTS_ATTR_PREFIX = 'inputs__'
// A test run nests the steps under `configuration`.
const STEP_INPUTS_ATTR = /^(?:configuration__)?actions__(.+?)__inputs__(.+)$/

function refusalData(error: unknown, code: string): { attr: string; detail: string } | null {
    const data = (error as { data?: { code?: unknown; attr?: unknown; detail?: unknown } } | null)?.data
    if (data?.code !== code || typeof data.attr !== 'string' || typeof data.detail !== 'string') {
        return null
    }
    return { attr: data.attr, detail: data.detail }
}

function parseInputRefusal(error: unknown, code: string): InputRefusal | null {
    const data = refusalData(error, code)
    if (!data?.attr.startsWith(INPUTS_ATTR_PREFIX)) {
        return null
    }
    return { key: data.attr.slice(INPUTS_ATTR_PREFIX.length), detail: data.detail }
}

export function parseSecretReentryRefusal(error: unknown): InputRefusal | null {
    return parseInputRefusal(error, SECRET_REENTRY_CODE)
}

// Entering a value again does not fix a connection refusal, so the form marks the input instead of asking.
export function parseConnectionRefusal(error: unknown): InputRefusal | null {
    return parseInputRefusal(error, CONNECTION_REFUSAL_CODE)
}

/** A refusal of one input of a workflow step, which the API reports at `actions__<step id>__inputs__<key>`. */
export function parseStepInputRefusal(error: unknown): StepInputRefusal | null {
    for (const code of [SECRET_REENTRY_CODE, CONNECTION_REFUSAL_CODE] as const) {
        const data = refusalData(error, code)
        const match = data?.attr.match(STEP_INPUTS_ATTR)
        if (data && match) {
            return { stepId: match[1], key: match[2], detail: data.detail, code }
        }
    }
    return null
}
