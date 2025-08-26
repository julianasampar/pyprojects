{{ config(materialized='table') }}

SELECT 1 AS tree_species_key, 'honeylocust' AS species_common_name, 'gleditsia triacanthos var. inermis' AS species_latin_name
UNION ALL
SELECT 2, 'american linden', 'tilia americana'
UNION ALL
SELECT 3, 'willow oak', 'quercus phellos'
UNION ALL
SELECT 4, 'london planetree', 'platanus x acerifolia'
UNION ALL
SELECT 5, 'pin oak', 'quercus palustris'
UNION ALL
SELECT 6, 'american elm', 'ulmus americana'
UNION ALL
SELECT 7, 'ash', 'fraxinus'
UNION ALL
SELECT 8, 'red maple', 'acer rubrum'
UNION ALL
SELECT 9, 'ginkgo', 'ginkgo biloba'
UNION ALL
SELECT 10, 'crab apple', 'malus'
UNION ALL
SELECT 11, NULL, NULL
UNION ALL
SELECT 12, 'turkish hazelnut', 'corylus colurna'
UNION ALL
SELECT 13, 'norway maple', 'acer platanoides'
UNION ALL
SELECT 14, 'sophora', 'styphnolobium japonicum'
UNION ALL
SELECT 15, 'callery pear', 'pyrus calleryana'
UNION ALL
SELECT 16, 'swamp white oak', 'quercus bicolor'
UNION ALL
SELECT 17, 'japanese zelkova', 'zelkova serrata'
UNION ALL
SELECT 18, 'sweetgum', 'liquidambar styraciflua'
UNION ALL
SELECT 19, 'ohio buckeye', 'aesculus glabra'
UNION ALL
SELECT 20, 'mulberry', 'morus'
UNION ALL
SELECT 21, 'tulip-poplar', 'liriodendron tulipifera'
UNION ALL
SELECT 22, 'northern red oak', 'quercus rubra'
UNION ALL
SELECT 23, 'amur maple', 'acer ginnala'
UNION ALL
SELECT 24, 'sawtooth oak', 'quercus acutissima'
UNION ALL
SELECT 25, 'holly', 'ilex'
UNION ALL
SELECT 26, 'littleleaf linden', 'tilia cordata'
UNION ALL
SELECT 27, 'black cherry', 'prunus serotina'
UNION ALL
SELECT 28, 'black oak', 'quercus velutina'
UNION ALL
SELECT 29, 'silver linden', 'tilia tomentosa'
UNION ALL
SELECT 30, 'green ash', 'fraxinus pennsylvanica'
UNION ALL
SELECT 31, 'hardy rubber tree', 'eucommia ulmoides'
UNION ALL
SELECT 32, 'black walnut', 'juglans nigra'
UNION ALL
SELECT 33, 'golden raintree', 'koelreuteria paniculata'
UNION ALL
SELECT 34, 'black locust', 'robinia pseudoacacia'
UNION ALL
SELECT 35, 'white oak', 'quercus alba'
UNION ALL
SELECT 36, 'cherry', 'prunus'
UNION ALL
SELECT 37, 'eastern hemlock', 'tsuga canadensis'
UNION ALL
SELECT 38, 'schumard''s oak', 'quercus shumardii'
UNION ALL
SELECT 39, 'siberian elm', 'ulmus pumila'
UNION ALL
SELECT 40, 'eastern cottonwood', 'populus deltoides'
UNION ALL
SELECT 41, 'hawthorn', 'crataegus'
UNION ALL
SELECT 42, 'purple-leaf plum', 'prunus cerasifera'
UNION ALL
SELECT 43, 'tree of heaven', 'ailanthus altissima'
UNION ALL
SELECT 44, 'chinese elm', 'ulmus parvifolia'
UNION ALL
SELECT 45, 'white ash', 'fraxinus americana'
UNION ALL
SELECT 46, 'kentucky coffeetree', 'gymnocladus dioicus'
UNION ALL
SELECT 47, 'shingle oak', 'quercus imbricaria'
UNION ALL
SELECT 48, 'japanese tree lilac', 'syringa reticulata'
UNION ALL
SELECT 49, 'chinese chestnut', 'castanea mollissima'
UNION ALL
SELECT 50, 'weeping willow', 'salix babylonica'
UNION ALL
SELECT 51, 'magnolia', 'magnolia'
UNION ALL
SELECT 52, 'english oak', 'quercus robur'
UNION ALL
SELECT 53, 'southern magnolia', 'magnolia grandiflora'
UNION ALL
SELECT 54, 'schubert''s chokecherry', 'prunus virginiana'
UNION ALL
SELECT 55, 'paper birch', 'betula papyrifera'
UNION ALL
SELECT 56, 'silver maple', 'acer saccharinum'
UNION ALL
SELECT 57, 'river birch', 'betula nigra'
UNION ALL
SELECT 58, 'eastern redcedar', 'juniperus virginiana'
UNION ALL
SELECT 59, 'dawn redwood', 'metasequoia glyptostroboides'
UNION ALL
SELECT 60, 'common hackberry', 'celtis occidentalis'
UNION ALL
SELECT 61, 'katsura tree', 'cercidiphyllum japonicum'
UNION ALL
SELECT 62, 'serviceberry', 'amelanchier'
UNION ALL
SELECT 63, 'bald cypress', 'taxodium distichum'
UNION ALL
SELECT 64, 'european beech', 'fagus sylvatica'
UNION ALL
SELECT 65, 'paperbark maple', 'acer griseum'
UNION ALL
SELECT 66, 'oklahoma redbud', 'cercis reniformis'
UNION ALL
SELECT 67, 'american hophornbeam', 'ostrya virginiana'
UNION ALL
SELECT 68, 'maple', 'acer'
UNION ALL
SELECT 69, 'flowering dogwood', 'cornus florida'
UNION ALL
SELECT 70, 'scots pine', 'pinus sylvestris'
UNION ALL
SELECT 71, 'boxelder', 'acer negundo'
UNION ALL
SELECT 72, 'american hornbeam', 'carpinus caroliniana'
UNION ALL
SELECT 73, 'eastern redbud', 'cercis canadensis'
UNION ALL
SELECT 74, 'empress tree', 'paulownia tomentosa'
UNION ALL
SELECT 75, 'black maple', 'acer nigrum'
UNION ALL
SELECT 76, 'sugar maple', 'acer saccharum'
UNION ALL
SELECT 77, 'silver birch', 'betula pendula'
UNION ALL
SELECT 78, 'cornelian cherry', 'cornus mas'
UNION ALL
SELECT 79, 'scarlet oak', 'quercus coccinea'
UNION ALL
SELECT 80, 'black pine', 'pinus nigra'
UNION ALL
SELECT 81, 'blackgum', 'nyssa sylvatica'
UNION ALL
SELECT 82, 'douglas-fir', 'pseudotsuga menziesii'
UNION ALL
SELECT 83, 'japanese maple', 'acer palmatum'
UNION ALL
SELECT 84, 'cucumber magnolia', 'magnolia acuminata'
UNION ALL
SELECT 85, 'kentucky yellowwood', 'cladrastis kentukea'
UNION ALL
SELECT 86, 'horse chestnut', 'aesculus hippocastanum'
UNION ALL
SELECT 87, 'catalpa', 'catalpa'
UNION ALL
SELECT 88, 'arborvitae', 'thuja occidentalis'
UNION ALL
SELECT 89, 'sycamore maple', 'acer pseudoplatanus'
UNION ALL
SELECT 90, 'amur maackia', 'maackia amurensis'
UNION ALL
SELECT 91, 'carpinus betulus', 'european hornbeam'
UNION ALL
SELECT 92, 'quercus macrocarpa', 'bur oak'
UNION ALL
SELECT 93, 'chionanthus retusus', 'chinese fringetree'
UNION ALL
SELECT 94, 'populus grandidentata', 'bigtooth aspen'
UNION ALL
SELECT 95, 'pinus virginiana', 'virginia pine'
UNION ALL
SELECT 96, 'lagerstroemia', 'crepe myrtle'
UNION ALL
SELECT 97, 'phellodendron amurense', 'amur cork tree'
UNION ALL
SELECT 98, 'acer campestre', 'hedge maple'
UNION ALL
SELECT 99, 'picea abies', 'norway spruce'
UNION ALL
SELECT 100, 'styrax japonicus', 'japanese snowbell'
UNION ALL
SELECT 101, 'acer tataricum', 'tartar maple'
UNION ALL
SELECT 102, 'quercus falcata', 'southern red oak'
UNION ALL
SELECT 103, 'cornus kousa', 'kousa dogwood'
UNION ALL
SELECT 104, 'sassafras albidum', 'sassafras'
UNION ALL
SELECT 105, 'larix laricina', 'american larch'
UNION ALL
SELECT 106, 'taxodium ascendens', 'pond cypress'
UNION ALL
SELECT 107, 'fagus grandifolia', 'american beech'
UNION ALL
SELECT 108, 'picea pungens', 'blue spruce'
UNION ALL
SELECT 109, 'chamaecyparis thyoides', 'atlantic white cedar'
UNION ALL
SELECT 110, 'carpinus japonica', 'japanese hornbeam'
UNION ALL
SELECT 111, 'pinus', 'pine'
UNION ALL
SELECT 112, 'alnus glutinosa', 'european alder'
UNION ALL
SELECT 113, 'acer platanoides ''crimson king''', 'crimson king maple'
UNION ALL
SELECT 114, 'halesia diptera', 'two-winged silverbell'
UNION ALL
SELECT 115, 'cornus alternifolia', 'pagoda dogwood'
UNION ALL
SELECT 116, 'aesculus x carnea', 'red horse chestnut'
UNION ALL
SELECT 117, 'carya glabra', 'pignut hickory'
UNION ALL
SELECT 118, 'cedrus deodara', 'himalayan cedar'
UNION ALL
SELECT 119, 'pinus rigida', 'pitch pine'
UNION ALL
SELECT 120, 'albizia julibrissin', 'mimosa'
UNION ALL
SELECT 121, 'pinus strobus', 'white pine'
UNION ALL
SELECT 122, 'populus tremuloides', 'quaking aspen'
UNION ALL
SELECT 123, 'picea', 'spruce'
UNION ALL
SELECT 124, 'maclura pomifera', 'osage-orange'
UNION ALL
SELECT 125, 'syringa pekinensis', 'chinese tree lilac'
UNION ALL
SELECT 126, 'pinus resinosa', 'red pine'
UNION ALL
SELECT 127, 'parrotia persica', 'persian ironwood'
UNION ALL
SELECT 128, 'crataegus crusgalli var. inermis', 'cockspur hawthorn'
UNION ALL
SELECT 129, 'cotinus coggygria', 'smoketree';
