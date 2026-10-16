import './assets/main.css'

import { createApp } from 'vue'
import App from './App.vue'
import router from './router'
import ElementPlus from 'element-plus'
import 'element-plus/dist/index.css'
import * as ElementPlusIconsVue from '@element-plus/icons-vue'
import './style.scss'
import API from '@/api/index'
import store from './store'
// import ViewUIPlus from 'view-ui-plus'
// import 'view-ui-plus/dist/styles/viewuiplus.css'


const app = createApp(App)

// 把封装好的接口对象挂到 Vue 全局，让所有组件都能用 $API 调接口，省去每次 import。
app.config.globalProperties.$API = API
// 屏蔽警告信息
app.config.warnHandler = () => null;
app.use(router)
app.use(ElementPlus)
app.use(store)
// app.use(ViewUIPlus)
//  把 Element Plus 的所有图标，批量注册成全局组件，让你在任何地方都能直接写 <Edit />、<Search />，不用 import。
for (const [key, component] of Object.entries(ElementPlusIconsVue)) {
  app.component(key, component)
}

app.mount('#app')
