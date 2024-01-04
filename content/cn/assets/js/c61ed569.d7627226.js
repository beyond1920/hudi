"use strict";(self.webpackChunkhudi=self.webpackChunkhudi||[]).push([[17068],{3905:(e,t,r)=>{r.d(t,{Zo:()=>d,kt:()=>m});var a=r(67294);function n(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function i(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,a)}return r}function o(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?i(Object(r),!0).forEach((function(t){n(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):i(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function s(e,t){if(null==e)return{};var r,a,n=function(e,t){if(null==e)return{};var r,a,n={},i=Object.keys(e);for(a=0;a<i.length;a++)r=i[a],t.indexOf(r)>=0||(n[r]=e[r]);return n}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(a=0;a<i.length;a++)r=i[a],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(n[r]=e[r])}return n}var l=a.createContext({}),u=function(e){var t=a.useContext(l),r=t;return e&&(r="function"==typeof e?e(t):o(o({},t),e)),r},d=function(e){var t=u(e.components);return a.createElement(l.Provider,{value:t},e.children)},p="mdxType",c={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},h=a.forwardRef((function(e,t){var r=e.components,n=e.mdxType,i=e.originalType,l=e.parentName,d=s(e,["components","mdxType","originalType","parentName"]),p=u(r),h=n,m=p["".concat(l,".").concat(h)]||p[h]||c[h]||i;return r?a.createElement(m,o(o({ref:t},d),{},{components:r})):a.createElement(m,o({ref:t},d))}));function m(e,t){var r=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var i=r.length,o=new Array(i);o[0]=h;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s[p]="string"==typeof e?e:n,o[1]=s;for(var u=2;u<i;u++)o[u]=r[u];return a.createElement.apply(null,o)}return a.createElement.apply(null,r)}h.displayName="MDXCreateElement"},8368:(e,t,r)=>{r.r(t),r.d(t,{contentTitle:()=>o,default:()=>p,frontMatter:()=>i,metadata:()=>s,toc:()=>l});var a=r(87462),n=(r(67294),r(3905));const i={title:"Querying Tables",keywords:["hudi","writing","reading"]},o="Querying Tables",s={unversionedId:"faq_querying_tables",id:"version-0.14.1/faq_querying_tables",title:"Querying Tables",description:"Does deleted records appear in Hudi's incremental query results?",source:"@site/versioned_docs/version-0.14.1/faq_querying_tables.md",sourceDirName:".",slug:"/faq_querying_tables",permalink:"/cn/docs/faq_querying_tables",editUrl:"https://github.com/apache/hudi/tree/asf-site/website/versioned_docs/version-0.14.1/faq_querying_tables.md",tags:[],version:"0.14.1",frontMatter:{title:"Querying Tables",keywords:["hudi","writing","reading"]},sidebar:"docs",previous:{title:"Writing Tables",permalink:"/cn/docs/faq_writing_tables"},next:{title:"Table Services",permalink:"/cn/docs/faq_table_services"}},l=[{value:"Does deleted records appear in Hudi&#39;s incremental query results?",id:"does-deleted-records-appear-in-hudis-incremental-query-results",children:[],level:3},{value:"How do I pass hudi configurations to my beeline Hive queries?",id:"how-do-i-pass-hudi-configurations-to-my-beeline-hive-queries",children:[],level:3},{value:"Does Hudi guarantee consistent reads? How to think about read optimized queries?",id:"does-hudi-guarantee-consistent-reads-how-to-think-about-read-optimized-queries",children:[],level:3}],u={toc:l},d="wrapper";function p(e){let{components:t,...r}=e;return(0,n.kt)(d,(0,a.Z)({},u,r,{components:t,mdxType:"MDXLayout"}),(0,n.kt)("h1",{id:"querying-tables"},"Querying Tables"),(0,n.kt)("h3",{id:"does-deleted-records-appear-in-hudis-incremental-query-results"},"Does deleted records appear in Hudi's incremental query results?"),(0,n.kt)("p",null,"Soft Deletes (unlike hard deletes) do appear in the incremental pull query results. So, if you need a mechanism to propagate deletes to downstream tables, you can use Soft deletes."),(0,n.kt)("h3",{id:"how-do-i-pass-hudi-configurations-to-my-beeline-hive-queries"},"How do I pass hudi configurations to my beeline Hive queries?"),(0,n.kt)("p",null,"If Hudi's input format is not picked the returned results may be incorrect. To ensure correct inputformat is picked, please use ",(0,n.kt)("inlineCode",{parentName:"p"},"org.apache.hadoop.hive.ql.io.HiveInputFormat")," or ",(0,n.kt)("inlineCode",{parentName:"p"},"org.apache.hudi.hadoop.hive.HoodieCombineHiveInputFormat")," for ",(0,n.kt)("inlineCode",{parentName:"p"},"hive.input.format")," config. This can be set like shown below:"),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-plain"},"set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat\n")),(0,n.kt)("p",null,"or"),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-plain"},"set hive.input.format=org.apache.hudi.hadoop.hive.HoodieCombineHiveInputFormat\n")),(0,n.kt)("h3",{id:"does-hudi-guarantee-consistent-reads-how-to-think-about-read-optimized-queries"},"Does Hudi guarantee consistent reads? How to think about read optimized queries?"),(0,n.kt)("p",null,"Hudi does offer consistent reads. To read the latest snapshot of a MOR table, a user should use snapshot query. The ",(0,n.kt)("a",{parentName:"p",href:"/docs/table_types#query-types"},"read-optimized queries")," (targeted for the MOR table ONLY) are an add on benefit to provides users with a practical tradeoff of decoupling writer performance vs query performance, leveraging the fact that most queries query say the most recent data in the table."),(0,n.kt)("p",null,"Hudi\u2019s read-optimized query is targeted for the MOR table only, with guidance around how compaction should be run to achieve predictable results. In the MOR table, the compaction, which runs every few commits (or \u201cdeltacommit\u201d to be exact for the MOR table) by default, merges the base (parquet) file and corresponding change log files to a new base file within each file group, so that the snapshot query serving the latest data immediately after compaction reads the base files only.\xa0 Similarly, the read-optimized query always reads the base files only as of the latest compaction commit, usually a few commits before the latest commit, which is still a valid table state."),(0,n.kt)("p",null,"Users must use snapshot queries to read the latest snapshot of a MOR table.\xa0 Popular engines including Spark, Presto, and Hive already support snapshot queries on MOR table and the snapshot query support in Trino is in progress (the ",(0,n.kt)("a",{parentName:"p",href:"https://github.com/trinodb/trino/pull/14786"},"PR")," is under review).\xa0 Note that the read-optimized query does not apply to the COW table."))}p.isMDXComponent=!0}}]);