import '@testing-library/jest-dom/vitest'
import {webcrypto} from 'node:crypto'
Object.defineProperty(globalThis, 'crypto', {value: webcrypto, configurable: true})
Object.defineProperty(window, 'matchMedia', {value: () => ({
  matches: false, addListener() {}, removeListener() {}, addEventListener() {}, removeEventListener() {},
})})
