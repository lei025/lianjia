from django.shortcuts import render
from show.models import  lianjia_House
from django.core.paginator import Paginator
from django.http import HttpResponseRedirect
# Create your views here.


def index(request):

    limit = 30
    house = lianjia_House.objects
    paginator = Paginator(house, limit)
    page_num = request.GET.get('page', 1)
    loaded = paginator.page(page_num)
    context = {
        'house':loaded

    }

    return render(request, 'index.html', context)



