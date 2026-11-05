import React from 'react'
import {createRoot} from 'react-dom/client'
import {BaseStyles, ThemeProvider} from '@primer/react'
import '@primer/primitives/dist/css/functional/themes/light.css'
import '@primer/primitives/dist/css/functional/themes/dark.css'
import {App} from './App'
import './style.css'

createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <ThemeProvider colorMode="auto" dayScheme="light" nightScheme="dark">
      <BaseStyles><App /></BaseStyles>
    </ThemeProvider>
  </React.StrictMode>,
)
